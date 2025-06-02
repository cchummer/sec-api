import logging
import requests
from collections import deque
from time import time, sleep
import config.settings as settings

class RateLimiter:
    '''
    Rate limiter using a token bucket algorithm. TODO: Will need slight update to work async
    '''

    def __init__(self, rate_limit):
        self.rate_limit = rate_limit  # Requests per second
        self.tokens = deque()  # Timestamps of recent requests
        self.request_count = 0 # For logging

    def acquire(self):
        while True:
            now = time()
            # Remove tokens older than 1 second
            while self.tokens and now - self.tokens[0] >= 1:
                self.tokens.popleft()
            # If we have space for a new token, add it and proceed
            if len(self.tokens) < self.rate_limit:
                self.tokens.append(now)
                self.request_count += 1
                logging.info(f"Request token {self.request_count} issued at {now}")
                break
            # Otherwise, wait until the next token is available
            sleep(1 / self.rate_limit)

def create_sec_rate_limiter():
    return RateLimiter(settings.SEC_RATE_LIMIT)

def download_file_from_sec(url, rate_limiter, max_retries=3, base_delay=1.0):
    '''
    Downloads the content of the file hosted at the specified SEC URL, abiding by rate limit and header spec rules.
    '''
    logging.info(f'Attempting to download SEC file: {url}')

    # Wait for rate limiter. TODO: Make async with 'await'
    if isinstance(rate_limiter, RateLimiter):
        rate_limiter.acquire()

        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=settings.SEC_REQ_HEADERS)
                if response.status_code == 200:
                    logging.info(f'Successfully fetched file contents from {url} on attempt {attempt+1}.')
                    return response.text
                else:
                    logging.warning(f"Attempt {attempt+1}: Failed to fetch {url} - Status {response.status}. Retrying...")
            
            except Exception as e:
                logging.warning(f"Attempt {attempt+1}: Error fetching {url}: {e}. Retrying...")
            
            if attempt < max_retries - 1:
                sleep(base_delay * (2 ** attempt))  # Exponential backoff

        logging.error(f"Failed to fetch {url} after {max_retries} attempts.")
    else:
        logging.error(f'No valid RateLimiter object was provided to download_file_from_sec. Cannot download.')
    return None
