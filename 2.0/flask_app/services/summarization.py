import logging
import nltk # TODO UPDATE REQUIREMENTS.TXT!?
import concurrent.futures
from time import time
from transformers import pipeline, AutoTokenizer # TODO UPDATE REQUIREMENTS.TXT!?

nltk.download('punkt_tab')

import config.settings as settings

def chunk_text_preserving_sentences(text, tokenizer, max_tokens=500, overlap=50):
    """
    Splits text into chunks while ensuring sentences are not broken, 
    unless a single sentence exceeds max_tokens, in which case it is split.
    """
    sentences = nltk.sent_tokenize(text)  # Split text into sentences
    
    chunks = []
    current_chunk = []
    current_length = 0

    for sentence in sentences:
        sentence_tokens = tokenizer.encode(sentence, add_special_tokens=False)

        # If a single sentence is too long, split it manually
        if len(sentence_tokens) > max_tokens:
            print(f"Warning: A single sentence exceeds max_tokens ({len(sentence_tokens)} tokens). Splitting it.")
            for i in range(0, len(sentence_tokens), max_tokens - overlap):
                chunk_tokens = sentence_tokens[i:i + max_tokens]
                chunks.append(tokenizer.decode(chunk_tokens))
            continue  # Skip to next sentence

        # If adding this sentence would exceed max_tokens, save current chunk and start a new one
        if current_length + len(sentence_tokens) > max_tokens:
            chunks.append(tokenizer.decode(sum(current_chunk, [])))  # Flatten token lists
            current_chunk = [sentence_tokens]  # Start new chunk
            current_length = len(sentence_tokens)
        else:
            current_chunk.append(sentence_tokens)
            current_length += len(sentence_tokens)

    # Add any remaining chunk
    if current_chunk:
        chunks.append(tokenizer.decode(sum(current_chunk, [])))

    print(f'Text chunked into {len(chunks)} part(s), preserving sentences when possible.')
    
    return chunks

def compute_summary_length(chunk_length, min_ratio=0.15, max_ratio=0.4, max_cap=200):
    """
    Computes dynamic summary min and max length based on the chunk size.
    
    - min_ratio: Minimum summary length as a fraction of chunk length.
    - max_ratio: Maximum summary length as a fraction of chunk length.
    - max_cap: Maximum allowable summary length.
    
    Returns (min_length, max_length).
    """
    max_length = min(int(chunk_length * max_ratio), max_cap)  # Scale and cap
    min_length = max(int(chunk_length * min_ratio), 20)  # Ensure a reasonable minimum
    return min_length, max_length

def summarize_chunk(chunk, tokenizer, summarizer, min_length, max_length, emotion='simple'):
    """
    Summarizes a single chunk of text.
    """
    if not chunk:
        return ""
    
    print(f'Summarizing {emotion} chunk. Tokenized length: {len(tokenizer.encode(chunk, add_special_tokens=False))}')
    
    summary = summarizer(chunk, min_length=min_length, max_length=max_length, do_sample=False)
    return summary[0]['summary_text']

def simple_summarize_text_section(section_text):
    '''
    Method to summarize a text section using default tokenizer and summarizer models. Currently for testing in RAG pipeline. TODO: Experiment with parallelization
    '''
    logging.info(f'Running simple summary on section of length: {len(section_text)}.')
    
    # Load tokenizer + summarizer
    tokenizer = AutoTokenizer.from_pretrained(settings.DEFAULT_TOKENIZER_MODEL)
    summarizer = pipeline("summarization", model=settings.DEFAULT_SUMMARIZER_MODEL)
    logging.info(f'Loaded default tokenizer: {settings.DEFAULT_TOKENIZER_MODEL} and default summarizer: {settings.DEFAULT_SUMMARIZER_MODEL}')

    chunked_text = chunk_text_preserving_sentences(section_text, tokenizer)
    summaries = []
    for i, c in enumerate(chunked_text):
        logging.info(f'Processing section chunk {i+1}/{len(chunked_text)}. Chunk length: {len(c)}.')

        if not c:
            logging.info(f'Empty chunk encountered, skipping summarization.')
            continue

        # Determine min/max summary lengths
        tokenized_chunk = tokenizer.encode(c, add_special_tokens=False)
        chunk_length = len(tokenized_chunk)
        min_length, max_length = compute_summary_length(chunk_length)

        summary = summarize_chunk(c, tokenizer, summarizer, min_length, max_length)
        summaries.append(summary)
        logging.info(f'Summarized chunk {i+1}. Chunk length: {len(c)}. Summary length: {len(summary)}.')

    logging.info(f'Summarized all section chunks. Combining summaries and returning.')
    return ' '.join(summaries)

def summarize_sentences_parallel(separated_sentences, tokenizer, summarizer, emotion):
    '''
    Original version/implementation of above simple_summarize_text_section from earlier flask app. Uses multithreading and is build to handle sentiment inference also.
    '''
    # Re-chunk sentences. Right now they are all separated in an array, we want to put them into ~512 token chunks, with chunks ending at sentence boundaries.
    emo_text = ' '.join(separated_sentences) 
    print(f'Re-chunking {len(separated_sentences)} {emotion} sentences. Made single string of len {len(emo_text)}: {emo_text}')
    chunked_emo_text = chunk_text_preserving_sentences(emo_text, tokenizer)

    summaries = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        print(f'Got ThreadPoolExecutor handle for summarization.')
        chunk_tasks = {}

        for i, c in enumerate(chunked_emo_text):
            print(f'Processing {emotion} sentence chunk {i + 1} / {len(chunked_emo_text)}.')
            
            if not c:
                print(f'Empty chunk encountered during summarization, skipping.')
                continue

            # Compute dynamic min/max summary lengths
            tokenized_chunk = tokenizer.encode(c, add_special_tokens=False)
            chunk_length = len(tokenized_chunk)
            min_length, max_length = compute_summary_length(chunk_length)

            print(f'Submitting {emotion} sentence chunk {i + 1}/{len(chunked_emo_text)} to ThreadPoolExecutor for summarization.\n'
                f'Chunk size: {chunk_length} tokens, Summary range: {min_length}-{max_length}')
            
            task = executor.submit(summarize_chunk, c, tokenizer, summarizer, min_length, max_length, emotion)
            chunk_tasks[task] = c
        
        for future in concurrent.futures.as_completed(chunk_tasks):
            chunk = chunk_tasks[future]  # Retrieve the original chunk
            try:
                summary = future.result()  # Get the completed summary
                summaries.append(summary)  # Store the result
            except Exception as e:
                print(f"Error summarizing chunk: {chunk}\n{e}")

    # Re-join to give us just a single string
    return ' '.join(summaries)