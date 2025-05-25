import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor
import random
from dataclasses import dataclass
from typing import Optional
import draccus
import logging
import fsspec
from pathlib import Path
import uuid
import gzip
from io import BytesIO
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ParallelScraperConfig:
    url_file: str
    output_warc: str
    num_workers: int = 8
    timeout: int = 5
    retry_count: int = 2
    random_seed: Optional[int] = 42
    chunk_size: int = 1000  # Process URLs in chunks

@draccus.wrap()
def main(config: ParallelScraperConfig):
    # Set random seed if specified
    if config.random_seed is not None:
        random.seed(config.random_seed)
    
    # Read URLs from file
    with fsspec.open(config.url_file, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]
    
    total_urls = len(urls)
    logger.info(f"Loaded {total_urls} URLs to scrape")
    
    # Create output directory if needed
    output_dir = os.path.dirname(config.output_warc)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Initialize WARC writer
    warc_path = config.output_warc
    if not warc_path.endswith('.warc.gz'):
        warc_path += '.warc.gz'
    
    # Process URLs in parallel chunks
    start_time = time.time()
    
    # Process in chunks to avoid memory issues with very large URL lists
    for i in range(0, len(urls), config.chunk_size):
        chunk = urls[i:i + config.chunk_size]
        logger.info(f"Processing chunk {i//config.chunk_size + 1}/{(len(urls) + config.chunk_size - 1)//config.chunk_size} ({len(chunk)} URLs)")
        
        # Process chunk in parallel
        with ThreadPoolExecutor(max_workers=config.num_workers) as executor:
            # Submit all tasks
            futures = [executor.submit(fetch_url, url, config.timeout, config.retry_count) for url in chunk]
            
            # Write results to WARC file as they complete
            mode = 'ab' if i > 0 and os.path.exists(warc_path) else 'wb'
            with fsspec.open(warc_path, mode, compression="infer") as output_file:
                for future, url in zip(futures, chunk):
                    try:
                        result = future.result()
                        if result:
                            status, headers, content = result
                            
                            # Create and write WARC record directly
                            record_data = create_warc_record_data(url, status, headers, content)
                            if record_data:
                                output_file.write(record_data)
                    except Exception as e:
                        logger.error(f"Error processing {url}: {str(e)}")
    
    elapsed_time = time.time() - start_time
    logger.info(f"Completed scraping {total_urls} URLs in {elapsed_time:.2f} seconds ({total_urls/elapsed_time:.2f} URLs/second)")

def fetch_url(url, timeout, retry_count):
    """Fetch a URL and return status code, headers, and content"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(retry_count + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout, stream=True)
            return response.status_code, dict(response.headers), response.content
        except Exception as e:
            if attempt < retry_count:
                # Add a small random delay before retrying
                time.sleep(5)
            else:
                logger.warning(f"Failed to fetch {url} after {retry_count + 1} attempts: {str(e)}")
                return None

def create_warc_record_data(url, status, headers, content):
    """Create a WARC record directly in the format expected by fastwarc"""
    try:
        # WARC headers
        record_id = str(uuid.uuid4())
        date = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        content_length = len(content)
        
        # Format HTTP status line and headers
        status_text = requests.status_codes._codes.get(status, [''])[0]
        http_headers = f"HTTP/1.1 {status} {status_text}\r\n"
        for name, value in headers.items():
            # Make sure header value doesn't contain newlines
            clean_value = re.sub(r'[\r\n]+', ' ', str(value))
            http_headers += f"{name}: {clean_value}\r\n"
        http_headers += "\r\n"
        
        # Combine HTTP headers and content
        payload = http_headers.encode('utf-8') + content
        
        # Create WARC record
        warc_record = (
            f"WARC/1.1\r\n"
            f"WARC-Type: response\r\n"
            f"WARC-Target-URI: {url}\r\n"
            f"WARC-Date: {date}\r\n"
            f"WARC-Record-ID: <urn:uuid:{record_id}>\r\n"
            f"Content-Type: application/http; msgtype=response\r\n"
            f"Content-Length: {len(payload)}\r\n"
            f"\r\n"
        ).encode('utf-8')
        
        # Combine WARC headers and payload
        warc_record += payload
        
        # Add record separator
        warc_record += b"\r\n\r\n"
        
        return warc_record
    except Exception as e:
        logger.error(f"Error creating WARC record for {url}: {str(e)}")
        return None

if __name__ == "__main__":
    main() 