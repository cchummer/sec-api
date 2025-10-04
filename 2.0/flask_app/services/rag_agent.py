"""
FROM GPT! TODO: IMPLEMENT
Agentic RAG layer for a Flask app backed by PostgreSQL + pgvector.

Capabilities:
- Classify NL queries into SQL / Semantic (RAG) / Hybrid
- Extract structured metadata filters (company, CIK, SIC/industry, filing type, date range, name-change flag)
- Rewrite/optimize queries for embedding-based retrieval
- Execute vector similarity with pgvector and optional metadata WHERE filters
- Execute grounded SQL for aggregates / facts (deterministic dashboards)
- Compose hybrid answers

Assumptions (adjust as needed):
- Table: filings(
    id bigserial PK,
    cik text,
    company_name text,
    sic text,               -- as code string, e.g. '2834'
    sic_desc text,
    filing_type text,       -- e.g. '10-K','10-Q','8-K','S-1','13F','6-K','13D','13G','DEF 14A','Form 4'
    filing_date date,
    name_history jsonb      -- optional: [{"name":"OldCo, Inc.", "start":"2001-01-01", "end":"2010-05-01"}, ...]
  )
- Table: filing_chunks(
    id bigserial PK,
    filing_id bigint REFERENCES filings(id) ON DELETE CASCADE,
    chunk_index int,
    content text,
    embedding vector(1536)  -- adapt to your model
  )

Dependencies:
  pip install psycopg[binary] pydantic python-dotenv
  (For OpenAI): pip install openai
  (For Anthropic): pip install anthropic

Environment variables (example):
  DATABASE_URL=postgresql+psycopg://user:pass@host:5432/dbname
  LLM_PROVIDER=openai            # or 'anthropic' or 'local'
  OPENAI_API_KEY=...
  OPENAI_MODEL=gpt-4o-mini
  ANTHROPIC_API_KEY=...
  ANTHROPIC_MODEL=claude-3-5-sonnet-latest

Indexes (run once in SQL, adjust opclass for metric):
  CREATE EXTENSION IF NOT EXISTS vector;
  -- Cosine distance
  CREATE INDEX IF NOT EXISTS idx_filing_chunks_hnsw_cosine
    ON filing_chunks USING hnsw (embedding vector_cosine_ops);
  -- Helpful metadata indexes
  CREATE INDEX IF NOT EXISTS idx_filings_date ON filings(filing_date);
  CREATE INDEX IF NOT EXISTS idx_filings_company ON filings(lower(company_name));
  CREATE INDEX IF NOT EXISTS idx_filings_sic ON filings(sic);
  CREATE INDEX IF NOT EXISTS idx_filings_type ON filings(filing_type);
  CREATE INDEX IF NOT EXISTS idx_chunks_filing_id ON filing_chunks(filing_id);
"""
from __future__ import annotations

import os
import json
import re
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple

import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

# ---------- LLM client abstraction ----------
class LLMClient:
    def __init__(self):
        self.provider = os.getenv("LLM_PROVIDER", "openai").lower()
        self.openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.anthropic_model = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
        if self.provider == "openai":
            from openai import OpenAI
            self.client = OpenAI()
        elif self.provider == "anthropic":
            import anthropic
            self.client = anthropic.Anthropic()
        else:
            self.client = None  # plug your local model here

    def chat_json(self, system: str, user: str, schema_hint: Optional[str] = None) -> Dict[str, Any]:
        """Call an LLM and return parsed JSON. Be defensive: if parsing fails, return {}."""
        content = user
        if self.provider == "openai":
            resp = self.client.chat.completions.create(
                model=self.openai_model,
                temperature=0.1,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": content},
                ],
                response_format={"type": "json_object"},
            )
            text = resp.choices[0].message.content
        elif self.provider == "anthropic":
            sys_msg = system
            resp = self.client.messages.create(
                model=self.anthropic_model,
                max_tokens=1024,
                temperature=0.1,
                system=sys_msg,
                messages=[{"role": "user", "content": content}],
            )
            # Anthropic returns a list of content blocks
            text = "".join(block.text for block in resp.content if hasattr(block, "text"))
        else:
            # Local fallback: naive rule-based JSON
            text = "{}"
        try:
            data = json.loads(text)
            if not isinstance(data, dict):
                return {}
            return data
        except Exception:
            return {}

    def chat_text(self, system: str, user: str) -> str:
        if self.provider == "openai":
            resp = self.client.chat.completions.create(
                model=self.openai_model,
                temperature=0.2,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            )
            return resp.choices[0].message.content or ""
        elif self.provider == "anthropic":
            resp = self.client.messages.create(
                model=self.anthropic_model,
                max_tokens=512,
                temperature=0.2,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
            return "".join(block.text for block in resp.content if hasattr(block, "text"))
        else:
            return user  # echo fallback

# ---------- Schemas ----------
class DateRange(BaseModel):
    start: Optional[str] = None  # 'YYYY-MM-DD'
    end: Optional[str] = None

class Filters(BaseModel):
    companies: List[str] = Field(default_factory=list)
    ciks: List[str] = Field(default_factory=list)
    sic_codes: List[str] = Field(default_factory=list)
    sic_keywords: List[str] = Field(default_factory=list)
    filing_types: List[str] = Field(default_factory=list)
    date_range: Optional[DateRange] = None
    name_change_only: bool = False

class RoutingDecision(BaseModel):
    mode: Literal["sql", "semantic", "hybrid"]
    rewritten_query: Optional[str] = None
    filters: Filters = Field(default_factory=Filters)

# ---------- DB helper ----------
class PG:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.getenv("DATABASE_URL")
        if not self.dsn:
            raise RuntimeError("DATABASE_URL not set")

    def connect(self):
        return psycopg.connect(self.dsn, row_factory=dict_row)

# ---------- Agent ----------
class RAGAgent:
    def __init__(self, db: PG, llm: Optional[LLMClient] = None, embed_model: str = "text-embedding-3-large"):
        self.db = db
        self.llm = llm or LLMClient()
        self.embed_model = embed_model
        self._openai_embed_client = None

    # --- public entrypoint ---
    def answer(self, user_query: str, top_k: int = 8) -> Dict[str, Any]:
        decision = self._route(user_query)
        if decision.mode == "sql":
            rows = self._run_sql(decision, user_query)
            return {"mode": "sql", "rows": rows}
        elif decision.mode == "semantic":
            docs = self._semantic_search(decision, top_k=top_k)
            final = self._synthesize_answer(user_query, docs, None)
            return {"mode": "semantic", "contexts": docs, "answer": final}
        else:  # hybrid
            rows = self._run_sql(decision, user_query)
            docs = self._semantic_search(decision, top_k=top_k)
            final = self._synthesize_answer(user_query, docs, rows)
            return {"mode": "hybrid", "rows": rows, "contexts": docs, "answer": final}

    # --- routing ---
    def _route(self, user_query: str) -> RoutingDecision:
        system = (
            "You classify SEC dashboard questions. Return JSON with keys: mode (sql|semantic|hybrid), "
            "rewritten_query (if semantic or hybrid), and filters. Filters include: companies[], ciks[], sic_codes[], "
            "sic_keywords[], filing_types[], date_range{start,end}, name_change_only (bool). "
            "Use SQL when question asks for counts, aggregates, time series, or deterministic statistics. "
            "Use semantic when the user wants passages, themes, risk factors, MD&A content, or opinions. "
            "Use hybrid when both are useful (e.g., "
            "'summarize top themes among 10-Ks in biotech last quarter and show counts by company')."
        )
        # Provide schema hints to improve extraction
        schema_hint = (
            "FILING TYPES: 10-K, 10-Q, 8-K, 6-K, 13F, 13D, 13G, S-1, S-3, DEF 14A, Form 4. "
            "SIC codes are 2-4 digit strings. Date range may be like 'last quarter', 'past week', 'Q2 2024'."
        )
        data = self.llm.chat_json(system, f"User query: {user_query}\n\nSchema hints: {schema_hint}")
        # Validate via Pydantic (and fill defaults)
        try:
            decision = RoutingDecision(**data)
        except Exception:
            # Fallback: guess
            mode = "sql" if re.search(r"count|how many|top|trend|by month|time series|increase|decrease", user_query, re.I) else "semantic"
            decision = RoutingDecision(mode=mode, rewritten_query=user_query, filters=Filters())
        # Light normalization of filing types
        if decision.filters.filing_types:
            decision.filters.filing_types = [ft.strip().upper() for ft in decision.filters.filing_types]
        return decision

    # --- SQL path ---
    def _build_where(self, f: Filters) -> Tuple[str, List[Any]]:
        clauses: List[str] = []
        params: List[Any] = []
        if f.companies:
            clauses.append("lower(company_name) = ANY($1)")
            params.append([c.lower() for c in f.companies])
        if f.ciks:
            clauses.append("cik = ANY($%d)" % (len(params) + 1))
            params.append(f.ciks)
        if f.sic_codes:
            clauses.append("sic = ANY($%d)" % (len(params) + 1))
            params.append([str(s) for s in f.sic_codes])
        if f.sic_keywords:
            # simple ILIKE on sic_desc
            like_params = [f"%{kw}%" for kw in f.sic_keywords]
            placeholders = ",".join([f"$%d" % (len(params) + i + 1) for i in range(len(like_params))])
            clauses.append(f"(" + " OR ".join(["sic_desc ILIKE %s" % p for p in placeholders.split(',')]) + ")")
            params.extend(like_params)
        if f.filing_types:
            clauses.append("upper(filing_type) = ANY($%d)" % (len(params) + 1))
            params.append([ft.upper() for ft in f.filing_types])
        if f.date_range and (f.date_range.start or f.date_range.end):
            if f.date_range.start:
                clauses.append("filing_date >= $%d" % (len(params) + 1))
                params.append(f.date_range.start)
            if f.date_range.end:
                clauses.append("filing_date <= $%d" % (len(params) + 1))
                params.append(f.date_range.end)
        if f.name_change_only:
            clauses.append("name_history IS NOT NULL AND jsonb_array_length(name_history) > 0")
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        return where, params

    def _run_sql(self, decision: RoutingDecision, user_query: str) -> List[Dict[str, Any]]:
        # Ask LLM to draft a safe, read-only SQL given schema hints
        system = (
            "You write SAFE, read-only SQL for PostgreSQL. Only select, no DML. Use the schema: "
            "filings(id, cik, company_name, sic, sic_desc, filing_type, filing_date, name_history jsonb), "
            "filing_chunks(id, filing_id, chunk_index, content, embedding). "
            "Only reference existing columns. Return just SQL, no explanation."
        )
        filter_text = self._filters_to_english(decision.filters)
        nl = f"Question: {user_query}\nFilters: {filter_text}\nReturn a compact SQL suited for small result sets (<= 200 rows)."
        sql_text = self.llm.chat_text(system, nl)
        # Safety: allowlist SELECT and common clauses only
        if not re.match(r"(?is)^\s*select\b", sql_text or ""):
            # fallback: basic count by filing_type
            sql_text = "SELECT filing_type, COUNT(*) AS n FROM filings GROUP BY 1 ORDER BY n DESC LIMIT 200;"
        # Inject WHERE based on Filters if caller SQL lacks one (simple heuristic)
        try:
            where_sql, params = self._build_where(decision.filters)
            if where_sql:
                lowered = sql_text.lower()
                if " where " not in lowered:
                    # Insert before GROUP BY/ORDER BY/LIMIT if present
                    m = re.search(r"(?is)\b(group\s+by|order\s+by|limit)\b", sql_text)
                    if m:
                        pos = m.start()
                        sql_text = sql_text[:pos] + where_sql + " " + sql_text[pos:]
                    else:
                        sql_text = sql_text.strip().rstrip(";") + where_sql + ";"
                else:
                    # If there is an existing WHERE, we AND our conditions
                    sql_text = re.sub(r"(?is)\bwhere\b", "WHERE (", sql_text, count=1)
                    sql_text = re.sub(r"(?is)(group\s+by|order\s+by|limit)\b", ") AND " + where_sql[7:] + r" \1", sql_text, count=1)
                    if ") AND " not in sql_text:
                        sql_text = sql_text.rstrip(";") + ") AND " + where_sql[7:] + ";"
        except Exception:
            params = []
        # Execute
        with self.db.connect() as conn:
            try:
                rows = list(conn.execute(sql_text, params))
            except Exception as e:
                rows = [{"error": str(e), "sql": sql_text}]
        return rows

    def _filters_to_english(self, f: Filters) -> str:
        parts = []
        if f.companies: parts.append(f"companies={f.companies}")
        if f.ciks: parts.append(f"ciks={f.ciks}")
        if f.sic_codes: parts.append(f"sic_codes={f.sic_codes}")
        if f.sic_keywords: parts.append(f"sic_keywords={f.sic_keywords}")
        if f.filing_types: parts.append(f"filing_types={f.filing_types}")
        if f.date_range: parts.append(f"date_range={f.date_range.model_dump()}")
        if f.name_change_only: parts.append("name_change_only=true")
        return ", ".join(parts) if parts else "(none)"

    # --- semantic path ---
    def _get_embed_client(self):
        if self._openai_embed_client is None:
            from openai import OpenAI
            self._openai_embed_client = OpenAI()
        return self._openai_embed_client

    def _embed(self, text: str) -> List[float]:
        client = self._get_embed_client()
        resp = client.embeddings.create(model=self.embed_model, input=text)
        return resp.data[0].embedding  # type: ignore

    def _optimize_query_for_embedding(self, query: str) -> str:
        system = (
            "Rewrite the user's question into a concise search query optimized for dense retrieval over SEC filings. "
            "Keep key entities (company names, tickers, CIKs), filing types, date/time constraints, and topic keywords. "
            "Remove filler and pronouns. Return a single line of text."
        )
        optimized = self.llm.chat_text(system, query)
        # Heuristic fallback if LLM returns empty
        if not optimized or len(optimized.strip()) < 4:
            optimized = re.sub(r"\b(what|which|show|tell|me|about|the|a|an|please|can|you)\b", " ", query, flags=re.I)
            optimized = re.sub(r"\s+", " ", optimized).strip()
        return optimized

    def _semantic_search(self, decision: RoutingDecision, top_k: int = 8) -> List[Dict[str, Any]]:
        q = decision.rewritten_query or ""
        optimized = self._optimize_query_for_embedding(q)
        vec = self._embed(optimized)
        where_sql, params = self._build_where(decision.filters)
        sql = (
            "SELECT fc.id as chunk_id, fc.filing_id, f.company_name, f.cik, f.filing_type, f.filing_date, "
            "       f.sic, f.sic_desc, fc.chunk_index, fc.content, "
            "       (fc.embedding <=> $%d) AS distance "
            "FROM filing_chunks fc JOIN filings f ON f.id = fc.filing_id %s "
            "ORDER BY fc.embedding <=> $%d ASC "
            "LIMIT %d;"
            % (len(params) + 1, where_sql, len(params) + 1, top_k)
        )
        with self.db.connect() as conn:
            rows = list(conn.execute(sql, [*params, vec]))
        # package contexts
        for r in rows:
            r["score"] = 1.0 - float(r.pop("distance", 0.0))  # cosine similarity approximation
        return rows

    # --- synthesis ---
    def _synthesize_answer(self, user_query: str, contexts: List[Dict[str, Any]], rows: Optional[List[Dict[str, Any]]]) -> str:
        # Clip long contexts
        def clip(txt: str, n: int = 1200):
            return txt[:n]
        ctx_snippets = [
            {
                "company": c.get("company_name"),
                "filing_type": c.get("filing_type"),
                "filing_date": str(c.get("filing_date")),
                "text": clip(c.get("content", "")),
            }
            for c in contexts
        ]
        system = (
            "You are a precise analyst of SEC filings. Answer concisely, cite companies and filing types/dates inline, "
            "and distinguish facts (from SQL rows) vs. interpretations (from contexts). If uncertain, say so."
        )
        user = (
            f"Question: {user_query}\n\n"
            f"Structured rows (if any):\n{json.dumps(rows or [], ensure_ascii=False)}\n\n"
            f"Retrieved contexts (top {len(ctx_snippets)}):\n{json.dumps(ctx_snippets, ensure_ascii=False)}\n\n"
            "Write 1-3 short paragraphs and bullet key points if helpful."
        )
        return self.llm.chat_text(system, user)

# ---------- Convenience: quick demo ----------
if __name__ == "__main__":
    db = PG()
    agent = RAGAgent(db)
    demo_q = "Summarize the main risk factors discussed in recent 10-Ks for biotech companies (SIC 2834) in Q2 2025, and which companies changed names."
    print(json.dumps(agent.answer(demo_q, top_k=6), indent=2, ensure_ascii=False))
