import nltk
import concurrent.futures
from time import time
from sqlalchemy import create_engine, text
from transformers import pipeline, AutoTokenizer

def chunk_text(text, tokenizer, max_tokens=500):
    """
    Chunks text into max_tokens token parts. 

    Returns a list, each item being a chunk of max specified size.
    """
    text_tokens = tokenizer.encode(text, add_special_tokens=False)
    chunked_text = []

    # Loop through tokens, chunking max_tokens at a time
    for i in range(0, len(text_tokens), max_tokens):
        chunk_tokens = text_tokens[i:i + max_tokens]
        chunk_text = tokenizer.decode(chunk_tokens)
        chunked_text.append(chunk_text)

    print(f'Text chunked into {len(chunked_text)} part(s).')
    
    return chunked_text

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

def find_emotional_sentences(text, tokenizer, classifier, emotions, threshold):
    sentences_by_emotion = {}
    for e in emotions:
        sentences_by_emotion[e]=[]
    sentences = nltk.sent_tokenize(text)
    print(f'Text has {len(text)} characters and {len(sentences)} sentences.')

    for i, s in enumerate(sentences):

        print(f'Attempting to analyze sentiment of sentence {i + 1} / {len(sentences)}.')

        # Chunk for BERT models
        chunked_sentence = chunk_text(s, tokenizer)

        sentence_emotion_label = 'unknown' # See TESTING NOTE below
        for i, sentence_chunk in enumerate(chunked_sentence):
            print(f'Analyzing sentence chunk {i + 1} / {len(chunked_sentence)}.\nsentence_emotion_label is currently: {sentence_emotion_label}.')

            if not sentence_chunk:
                print(f'Empty chunk encountered during emotion analysis, skipping.')
                continue

            # TESTING NOTE: If the first chunk of sentence if determined to be emotional, want to save the rest of it even if those chunks themselves dont register as emotional, to retain the meaning of the sentence...
            # This way relies on the emotion of the entire sentence being properly determined by the first emotional chunk it comes across, and in the case that emotion is not determined until the second chunk or later,
            # any content of the sentence before that chunk will not be saved. There is also the possibility for two chunks of the same sentence to be of different sentiment. However these are rather edge cases 
            # so I will address them as I get this current code working... Most sentences should be within ~512 tokens anyways or at least make known their sentiment by then but ya something to think about.
            prediction = classifier(sentence_chunk)
            chunk_emotion_label = prediction[0]['label']

            #print(f'Chunk emotion label: {chunk_emotion_label}.')

            if chunk_emotion_label != 'neutral' and prediction[0]['score'] > threshold:
                #print(f'Emotional chunk found: {chunk_emotion_label}. sentence_emotion_label will be updated accordingly (any further chunks of this sentence will be saved under this emotion). Chunk text: {sentence_chunk}')
                sentences_by_emotion[chunk_emotion_label].append(sentence_chunk)
                sentence_emotion_label = chunk_emotion_label
            elif sentence_emotion_label != 'unknown': # This chunk was not determined to be emotional, but it is a continuation of a sentence which was, so append it with that sentence. 
                #print(f'Unemotional chunk found after emotional sentence determined ({sentence_emotion_label}), saving it. Chunk text: {sentence_chunk}')
                sentences_by_emotion[sentence_emotion_label].append(sentence_chunk)
            else:
                pass#print(f'Chunk was not emotional, and is not part of a previously determined emotional sentence. Not being saved. Chunk text: {sentence_chunk}')

    for e in emotions:
        print(f'{e}: {len(sentences_by_emotion[e])} sentences.')
    return sentences_by_emotion
    
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

def summarize_sentences_parallel(separated_sentences, tokenizer, summarizer, emotion):

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

def sentiment_summarize_text_sentences(text, tokenizer, sentiment_analyzer, classifier_emotions, summarizer):
    """
    Performs sentiment analysis and summarization on a document section, tokenizing in sentences.

    Returns a dictionary, with one key per emotion which was detected in one more more sentences, and the values being summaries of sentences of the respective emotions.
    """
    #sentences_by_emotion = find_emotional_sentences(text, classifier_emotions, 0.7)
    sentences_by_emotion = find_emotional_sentences(text, tokenizer, sentiment_analyzer, classifier_emotions, 0.7)
    print(f'Ran sentence level sentiment analysis on text.')

    emotional_summary = {}
    for emotion in sentences_by_emotion:
        #print(f'{emotion} sentences: {sentences_by_emotion[emotion]}')
        #input()
        current_summary = summarize_sentences_parallel(sentences_by_emotion[emotion], tokenizer, summarizer, emotion)
        print(f'Ran summary analysis of {emotion} sentences.')
        emotional_summary[emotion] = current_summary
        #print(f'Summary of {emotion} sentences: {emotion_summary}')
        #input()

    print(f'Finished analyzing emotional sentences in text section.')
    return emotional_summary

def perform_filing_level_nlp(filing_data):
    '''
    Performs, at the moment, sentiment and summarization analysis of a filings text data, on a per section (or PDF page) basis 
    as provided in filing_data. 

    Returns a dictionary, one key per text type (again, currently either TextSectionFacts or PDFPageText), holding a list of analyzed entries
    '''
    # Load tokenizer
    tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")  # For chunking

    # Load sentiment and summarization models
    sentiment_analyzer = pipeline("text-classification", model="ProsusAI/finbert")
    classifier_emotions = ['positive', 'neutral', 'negative']
    summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

    # Analyze TextSectionFacts data
    print(f'Preparing to analyze TextSectionFacts of current filing.')
    t0 = time()
    analyzed_text_sections = []
    text_sections = filing_data.get('TextSectionFacts', [])
    for i, section in enumerate(text_sections):
        print(f'Analyzing text section {i + 1} / {len(text_sections)} of current filing.')
        t1 = time()
        section_info = {
            'section_id': section[0],
            'home_doc_id': section[1],
            'section_name': section[2],
            'section_type': section[3],
            'section_text': section[4],
            'emotional_summary': sentiment_summarize_text_sentences(section[4], tokenizer, sentiment_analyzer, classifier_emotions, summarizer)
        }
        analyzed_text_sections.append(section_info)
        print(f'Completed analysis of text section. Time on section: {(time() - t1):.3f}s. Total analysis time so far: {(time() - t0):.3f}s.')

    # And PDFPageText data (for now is the only other format that text based data from filings would be saved in the DB)
    print(f'Finished analyzing TextSectionFacts, time elapsed: {(time() - t0):.3f}s.\nPreparing to analyze PDFPageText entries of current filing.')
    analyzed_pdfs = []
    pdf_pages = filing_data.get('PDFPageText', [])
    for i, pdf_page in enumerate(pdf_pages):
        print(f'Analyzing PDF page {i + 1} / {len(pdf_pages)} of current filing.')
        t1 = time()
        page_info = {
            'page_id': pdf_page[0],
            'home_pdf_id': pdf_page[1],
            'page_num': pdf_page[2],
            'page_text': pdf_page[3],
            'emotional_summary': sentiment_summarize_text_sentences(pdf_page[3], tokenizer, sentiment_analyzer, classifier_emotions, summarizer)
        }
        analyzed_pdfs.append(page_info)
        print(f'Completed analysis of PDF page. Time on page: {(time() - t1):.3f}s. Total analysis time so far: {(time() - t0):.3f}s.')

    print(f'Finished analyzing PDFPageText entries, total time elapsed: {(time() - t0):.3f}s')

    return {
        'TextSectionFacts': analyzed_text_sections,
        'PDFPageText': analyzed_pdfs
    }

def perform_section_level_summary(filing_data, text_section_id):
    '''
    Performs, at the moment summarization analysis of a single text section of a filing.

    Returns a summary as a string.
    '''

    # Load tokenizer + summarizer
    tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
    summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

    print(f'Attempting section level basic summary of section ID {text_section_id}.')
    
    filing_text_sections = filing_data.get('TextSectionFacts', [])
    if not filing_text_sections:
        print(f'Filing data dict had no TextSectionFacts... no text to analyze.')
        return None

    for i, section in enumerate(filing_text_sections):
        # Look for our target section
        if int(section[0]) == int(text_section_id):
            print(f'Found target section ID {text_section_id}. Attempting to perform simple summary.')
            section_summary = summarize_sentences_parallel([section[4]], tokenizer, summarizer, 'simple')
            if section_summary:
                print('Generated simple section summary.')
                return section_summary
            else:
                print('Failed to generate section summary.')
                return None

def perform_document_level_summary(filing_data, doc_id):
    '''
    Performs, at the moment summarization analysis of a text document of a filing, comprised of one or more text sections.

    Returns a summary as a string.
    '''

    # Load tokenizer + summarizer
    tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
    summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

    print(f'Attempting document level basic summary of document ID {doc_id}.')
    
    filing_text_sections = filing_data.get('TextSectionFacts', [])
    if not filing_text_sections:
        print(f'Filing data dict had no TextSectionFacts... no text to analyze.')
        return None

    # We will build a string to represent the entire document
    doc_text = ''
    for i, section in enumerate(filing_text_sections):
        # Look for sections with home document ID's equal to our target
        if int(section[1]) == int(doc_id):
            print(f'Found section of target document ID {doc_id}. Appending to document text string.')
            print(f'Section id: {section[0]}, section name: {section[2]}, section type: {section[3]}.')
            doc_text += section[4]

    # Now summarize the whole thing
    print(f'Built document string of len {len(doc_text)} characters. Sending to simple summarizer.')
    doc_summary = summarize_sentences_parallel([doc_text], tokenizer, summarizer, 'simple')
    if doc_summary:
        print('Generated simple document summary.')
        return doc_summary
    else:
        print('Failed to generate document summary.')
        return None
            

def perform_section_level_sentiment_summary(filing_data, text_section_id):
    '''
    Performs, at the moment, sentiment and summarization analysis of a single text section of a filing.

    Returns a dictionary, one key per emotion detected, values being summaries of respectively emotional sentences.
    '''

    # Load tokenizer
    tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")

    # Load sentiment and summarization models
    sentiment_analyzer = pipeline("text-classification", model="ProsusAI/finbert")
    classifier_emotions = ['positive', 'neutral', 'negative']
    summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

    filing_text_sections = filing_data.get('TextSectionFacts', [])
    if not filing_text_sections:
        print(f'Filing data dict had no TextSectionFacts... no text to analyze.')
        return {}

    for i, section in enumerate(filing_text_sections):
        # Look for our target section
        if int(section[0]) == int(text_section_id):
            print(f'Found target section ID {text_section_id}. Attempting to perform sentiment summary.')
            sentiment_summary = sentiment_summarize_text_sentences(section[4], tokenizer, sentiment_analyzer, classifier_emotions, summarizer)
            if sentiment_summary:
                print('Generated sentiment summary for section.')
                return sentiment_summary
            else:
                print('Failed to generate sentiment summary for section.')
                return {}
            
    return {}