import json
import config
from time import time
from datetime import datetime
import matplotlib
matplotlib.use("Agg")  # Use a non-GUI backend
import matplotlib.pyplot as plt
import numpy as np
import spacy
import gensim
from gensim.models import CoherenceModel
from gensim.corpora import Dictionary
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from spacy.lang.en.stop_words import STOP_WORDS as SPACY_STOP_WORDS

nlp = spacy.load("en_core_web_sm", disable=["parser", "ner", "tagger"])
# Build combined stopword list
COMBINED_STOP_WORDS = set(ENGLISH_STOP_WORDS).union(set(config.SEC_STOP_WORDS)).union(SPACY_STOP_WORDS)

def plot_top_words(model, feature_names, n_top_words, title):
    num_topics = len(model.components_)  # Get the number of topics
    
    # Determine the number of rows and columns dynamically
    n_cols = min(5, num_topics)  # Limit to 5 columns for readability
    n_rows = int(np.ceil(num_topics / n_cols))  # Compute the required rows

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols * 6, n_rows * 6), sharex=True)
    axes = np.array(axes).flatten()  # Flatten in case of single row

    for topic_idx, topic in enumerate(model.components_):
        top_features_ind = topic.argsort()[-n_top_words:]
        top_features = feature_names[top_features_ind]
        weights = topic[top_features_ind]

        ax = axes[topic_idx]
        ax.barh(top_features, weights, height=0.7)
        ax.set_title(f"Topic {topic_idx + 1}", fontdict={"fontsize": 20})
        ax.tick_params(axis="both", which="major", labelsize=14)
        
        # Hide unnecessary spines
        for i in ["top", "right", "left"]:
            ax.spines[i].set_visible(False)

    # Hide unused subplots if any
    for i in range(num_topics, len(axes)):
        fig.delaxes(axes[i])

    fig.suptitle(title, fontsize=30)
    plt.subplots_adjust(top=0.9, bottom=0.05, wspace=0.5, hspace=0.5)

    # Save the plot
    filename = "static/topic_plot.png"
    plt.savefig(filename, bbox_inches="tight")
    plt.close(fig)

    print(f'Plot saved as {filename}.')
    return filename

def calculate_coherence(tfidf, tfidf_vectorizer, nmf, texts, n_topics_range=(5, 20)):
    """
    Calculate coherence scores for NMF models with varying numbers of topics.
    
    Args:
        tfidf: TF-IDF matrix (from `tfidf_vectorizer.fit_transform`).
        tfidf_vectorizer: Fitted TF-IDF vectorizer.
        nmf: NMF model (unfitted, used as a template).
        texts: List of preprocessed documents (tokenized).
        n_topics_range: Range of topic numbers to test.
    
    Returns:
        coherence_scores: List of coherence scores for each topic number.
        optimal_n_topics: Best number of topics based on coherence.
    """
    coherence_scores = []
    vocab = tfidf_vectorizer.get_feature_names_out()

    # Create a proper Gensim Dictionary
    dictionary = Dictionary([vocab])  # Wrap vocab in a list of lists

    print(f'Calculating coherence scores for corpus.')
    
    # Convert TF-IDF matrix to gensim-compatible corpus
    corpus = gensim.matutils.Sparse2Corpus(tfidf, documents_columns=False)
    
    for n_topics in range(n_topics_range[0], n_topics_range[1] + 1):
        # Fit NMF
        nmf.n_components = n_topics
        W = nmf.fit_transform(tfidf)
        H = nmf.components_
        
        # Extract top words per topic
        top_words_per_topic = []
        for topic_idx in range(n_topics):
            top_features_ind = H[topic_idx].argsort()[-10:][::-1]  # Top 10 words
            top_words = [vocab[i] for i in top_features_ind]
            top_words_per_topic.append(top_words)
        
        # Calculate coherence (c_v is a good default)
        coherence = CoherenceModel(
            topics=top_words_per_topic,
            texts=texts,  # Tokenized documents
            dictionary=dictionary,
            coherence='c_v',
            processes=1#testing
        ).get_coherence()
        
        coherence_scores.append(coherence)
        print(f"n_topics={n_topics}, Coherence={coherence:.3f}")
    
    # Optimal number: highest coherence
    optimal_n_topics = n_topics_range[0] + np.argmax(coherence_scores)
    return coherence_scores, optimal_n_topics

def find_optimal_nmf_topics(tfidf, n_topics_range=(5, 20)):
    n_samples, n_features = tfidf.shape
    max_topics = min(n_samples, n_features)  # Maximum topics allowed

    print(f"The min of n_samples and n_features: {max_topics}")

    # Adjust the topic range dynamically
    min_topics = max(2, min(n_topics_range[0], max_topics))  # At least 2 topics
    max_topics = min(n_topics_range[1], max_topics)

    if min_topics > max_topics:
        print("Warning: Not enough documents/features for the requested topic range.")
        return [], None

    errors = []

    for n_topics in range(min_topics, max_topics + 1):
        init_method = "nndsvd" if n_topics <= max_topics else "random"
        try:
            nmf = NMF(n_components=n_topics, init=init_method, random_state=42)
            nmf.fit(tfidf)
            error = nmf.reconstruction_err_
            errors.append(error)
            print(f"n_topics={n_topics}, Reconstruction Error={error:.4f}")
        except ValueError as e:
            print(f"Skipping n_topics={n_topics} due to error: {e}")
            continue  # Skip invalid topic numbers

    if not errors:
        return [], None  # No valid topics found

    # Find the lowest error
    optimal_n_topics = min_topics + np.argmin(errors)
    return errors, optimal_n_topics

def spacy_tokenize_and_lemmatize(text):
    """
    Enhanced tokenizer with:
    - SEC-specific pattern handling (10-K, Item 1A)
    - Comprehensive hyphen merging
    - Stop word removal (COMBINED_STOP_WORDS)
    - Numeric token exclusion
    - Lemmatization
    Returns: (tokenized_list, preprocessed_string)
    """
    doc = nlp(text.lower())
    tokens = []
    i = 0
    while i < len(doc):
        token = doc[i]
        
        # Skip stop words (check original text to avoid lemmatized forms)
        if token.text in COMBINED_STOP_WORDS:
            i += 1
            continue
            
        # Handle number-hyphen patterns (e.g., 10-K)
        if token.like_num and i < len(doc)-1 and doc[i+1].text == "-":
            if i < len(doc)-2:
                combined = f"{token.text}-{doc[i+2].text}"
                if combined not in COMBINED_STOP_WORDS:  # Check if combined form is stopword
                    tokens.append(combined)
                i += 3
            continue
            
        # Handle Item 1A patterns
        elif token.text == "item" and i < len(doc)-1 and doc[i+1].like_num:
            combined = f"item_{doc[i+1].text}"
            if combined not in COMBINED_STOP_WORDS:
                tokens.append(combined)
            i += 2
            continue
        
        # Comprehensive hyphen merging (e.g., "year-over-year")
        elif token.text == "-" and i > 0 and i < len(doc)-1:
            combined = f"{doc[i-1].lemma_}-{doc[i+1].lemma_}"
            if combined not in COMBINED_STOP_WORDS:
                tokens.append(combined)
            i += 2
            continue
        
        # Default case - exclude pure numbers, punctuation, and spaces
        elif (not token.is_punct and 
              not token.is_space and
              not token.like_num and  # This excludes pure numbers
              token.lemma_ not in COMBINED_STOP_WORDS):
            tokens.append(token.lemma_)
        i += 1
            
    return tokens, " ".join(tokens)

def topic_analyze_corpus(documents, additional_metadata={}):
    '''
    Runs a TF-IDF and NMF based topic analysis of the given corpus. Input 'texts' should be a list of documents / text sections
    Parameters:
      documents: List of dicts, each with keys:
            "doc_id" and "doc_text"
        to be used for analysis.
      analysis_date: Date to save with analysis metadata
      additional_metadata: A dict containing any extra metadata such as:
          - run_description, etc.
    
    Returns:
      A tuple (analysis_run_data, terms_list, document_topic_data, document_term_data, topic_term_data) where:
      
      - analysis_run_data is a dict with keys:
            "analysis_date", "topic_term_matrix" (JSON), "stopwords", "additional_metadata" (JSON),
            "n_topics", "n_top_words_doc", "corpus_size", "max_df", and "min_df"
      - terms_list is a list of dicts, each with keys:
            "term_id" and "word"
      - document_topic_data is a list of dicts, each with keys:
            "doc_id", "topic_id", and "topic_weight"
      - document_term_data is a list of dicts, each with keys:
            "doc_id", "term_id", and "tfidf_weight"
      - topic_term_data is a list of dicts, each with keys:
            "topic_id", "term_id", and "word_weight"
    '''

    print(f'Attempting NMF topic analysis run, additional metadata: {additional_metadata}')

    # Remove empty documents/sections
    documents = [doc for doc in documents if doc.get('doc_text', '').strip()]
    
    if not documents:
        print("All documents are empty or contain only stop words. Skipping topic analysis.")
        return None, None, None, None, None
    
    # Store original doc_id 
    documents_text = [doc.get('doc_text', '') for doc in documents]
    doc_ids = [doc.get('doc_id', 0000) for doc in documents]

    # Tokenize with spaCy
    tokenized_texts = []
    preprocessed_texts = []
    for i, doc in enumerate(documents_text):
        print(f'Tokenizing section/doc {i + 1} / {len(documents_text)}.')
        tokens, preprocessed = spacy_tokenize_and_lemmatize(doc)
        tokenized_texts.append(tokens)
        preprocessed_texts.append(preprocessed)
        print(f'Tokenized section/doc. Sample of original:\n{doc[:100]}\nSample of preprocessed: {preprocessed[:100]}')

    print("Extracting tf-idf features for NMF...")

    # Determine vectorization settings based on corpus size
    if len(documents_text) > 20:
        max_df = 0.95
        min_df = 5# changed from 3
    elif len(documents_text) > 10:
        max_df = 0.95
        min_df = 3# changed from 2
    else:
        max_df = 1.0
        min_df = 1

    tfidf_vectorizer = TfidfVectorizer(
        max_df=max_df,
        min_df=min_df,
        max_features=config.NMF_N_FEATURES,
        sublinear_tf=True,
        stop_words=None,
        token_pattern=None,
        tokenizer=lambda x: x.split(), # spacy_tokenize_and_lemmatize returns a space separated list of tokens, so we just split on spaces and rely on spacy's work
        preprocessor=None,
        ngram_range=(1,2)
    )

    try:
        t0 = time()
        tfidf = tfidf_vectorizer.fit_transform(preprocessed_texts)
        print("Done in %0.3fs." % (time() - t0))

    except ValueError as e:
        print(f"TF-IDF vectorization failed: {str(e)}")
        return None, None, None, None, None
    
    vocab = tfidf_vectorizer.get_feature_names_out()

    # TESTING
    print(f"Sample vocabulary: {list(vocab[:100])}")
    input()

    # Check for empty vocabulary
    if not vocab.size:
        print("Empty vocabulary: the document(s) may only contain stop words. Skipping topic analysis.")
        return None, None, None, None, None

    # Save terms/vocab 
    t0 = time()
    terms_list = [
        {
            'term_id': int(term_idx),
            'word': term
        }
        for term_idx, term in enumerate(vocab)
    ]
    print(f'Built terms_list. Done in {(time() - t0):.3f}s.')
    
    # Determine number of topics dynamically via coherence calculation with tokenized_texts
    '''
    nmf_template = NMF(init=config.NMF_INIT, beta_loss=config.NMF_BETA_LOSS, random_state=1)
    coherence_scores, optimal_n_topics = calculate_coherence(
        tfidf, tfidf_vectorizer, nmf_template, tokenized_texts, n_topics_range=(5, 15)
    )'
    '''
    # Testing reconstruction error
    errors, optimal_n_topics = find_optimal_nmf_topics(tfidf)

    print(f'Optimal number of topics: {optimal_n_topics}')
    input()

    # Fit the NMF model
    print("Fitting the NMF model with tf-idf features...")
    t0 = time()
    nmf = NMF(
        n_components=optimal_n_topics,
        random_state=42,
        init=config.NMF_INIT,
        beta_loss=config.NMF_BETA_LOSS,
        alpha_W=0.00005,
        alpha_H=0.00005,
        l1_ratio=1,
    ).fit(tfidf)
    print("Done in %0.3fs." % (time() - t0))

    plot_path = plot_top_words(nmf, vocab, 10, f"Topics in NMF {config.NMF_BETA_LOSS}")
    print(f'NMF Fitting was successful. See output: {plot_path}')
    input()

    # Transform the TF-IDF matrix to get document(section)-topic distribution
    W = nmf.transform(tfidf)  # shape: (n_documents, n_topics)

    # Build document_topic_data
    t0 = time()
    document_topic_data = [
        {"doc_id": int(doc_ids[doc_idx]), "topic_id": int(topic_idx), "topic_weight": weight}
        for doc_idx, row in enumerate(W)  # Iterate over documents
        for topic_idx, weight in enumerate(row)  # Iterate over topics in a document
        if weight > 0 # Only save relevant topics
    ]
    print(f'Built document_topic_data. Done in {(time() - t0):.3f}s.')

    # Build document_term_data
    t0 = time()
    document_term_data = []
    n_top_words = config.NMF_N_TOP_DOC_WORDS
    for doc_idx, doc_id in enumerate(doc_ids):
        # Get nonzero tfidf values and their corresponding term indices
        nonzero_indices = tfidf[doc_idx].nonzero()[1]
        term_weights = [(idx, tfidf[doc_idx, idx]) for idx in nonzero_indices]
        
        # Sort by tfidf weight and take the top N terms
        top_terms = sorted(term_weights, key=lambda x: x[1], reverse=True)[:n_top_words]

        # Store only top terms. TODO experiment with other methods of saving document-term relationships...
        for term_idx, tfidf_weight in top_terms:
            document_term_data.append({
                'doc_id': int(doc_id),
                'term_id': int(term_idx),
                'tfidf_weight': float(tfidf_weight)
            })
    print(f'Built document_term_data with top {n_top_words} per document. Done in {(time() - t0):.3f}s.')
    
    # Will hold topic-term relationships
    topic_term_data = []
    
    # Build topic_term_data
    # Loop over each topic (from the topic-word matrix)
    n_top_words = config.NMF_N_TOP_TOPIC_WORDS
    t0 = time()
    for topic_idx, topic in enumerate(nmf.components_):
        # Get indices for the top n words for this topic,
        # sorted in descending order by weight.
        top_features_ind = topic.argsort()[-n_top_words:][::-1]
        top_weights = topic[top_features_ind]

        # Build list of top words with weights.
        single_topic_term_data = [
            { "topic_id": int(topic_idx), "term_id": int(term_idx), "word_weight": float(weight) }
            for term_idx, weight in zip(top_features_ind, top_weights)
        ]

        # Add to topic_term_data.
        topic_term_data.extend(single_topic_term_data)
        
    print(f'Built topic_term_data with top {n_top_words} per topic. Done in  {(time() - t0):.3f}s.')
    
    # Prepare run-level metadata.
    t0 = time()
    analysis_run_data = {
        'analysis_datetime': datetime.now(),
        'stopwords': COMBINED_STOP_WORDS,
        'additional_metadata': json.dumps(additional_metadata),
        'n_topics': int(optimal_n_topics),
        'n_top_words_doc': int(config.NMF_N_TOP_DOC_WORDS),
        'n_top_words_topic': int(config.NMF_N_TOP_TOPIC_WORDS),
        'corpus_size': int(len(documents_text)),
        'max_df': int(max_df),
        'min_df': int(min_df),
    }
    print(f'Built analysis_run_data. Done in {(time() - t0):.3f}s.')

    print(f'Finished NMF topic analysis run with metadata {analysis_run_data}.')
    return analysis_run_data, terms_list, document_topic_data, document_term_data, topic_term_data