import asyncio
import aiohttp
import time
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from datetime import datetime
import logging
from collections import defaultdict
import heapq
import hashlib
from concurrent.futures import ThreadPoolExecutor
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO, # Changed to INFO to reduce noise, DEBUG is very verbose
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PriorityURL:
    """URL with priority for heap queue"""
    def __init__(self, url, depth, priority):
        self.url = url
        self.depth = depth
        self.priority = priority
        
    def __lt__(self, other):
        return self.priority < other.priority
    
    def __eq__(self, other):
        return self.url == other.url

class WebCrawler:
    def __init__(self, query, max_pages=500, max_depth=3, concurrent_requests=20, num_threads=4):
        self.query = query
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.concurrent_requests = concurrent_requests
        self.num_threads = num_threads
        
        # --- START: Consolidated Shared State ---
        # All shared data structures are now protected by a single lock to prevent deadlocks.
        self.visited_hashes = set()
        self.url_queue = []
        self.domain_counts = defaultdict(int)
        self.domain_last_visit = defaultdict(float)
        self.pages_crawled = 0
        # --- END: Consolidated Shared State ---
        
        self.start_time = None
        self.crawl_log_file = None
        
        self.shared_state_lock = threading.Lock()
        
    def get_url_hash(self, url):
        """Get MD5 hash of URL for efficient storage in the visited set"""
        return hashlib.md5(url.encode()).hexdigest()
    
    def _calculate_priority(self, url, depth, parent_domain=None):
        """
        Internal method to calculate URL priority.
        MUST be called within the shared_state_lock.
        """
        domain = urlparse(url).netloc
        priority = depth * 100
        
        # Penalize domains we've visited a lot
        priority += self.domain_counts[domain] * 50
        
        # Penalize recently visited domains to encourage domain switching
        time_since_visit = time.time() - self.domain_last_visit.get(domain, 0)
        if time_since_visit < 5:
            priority += 200
            
        # Bonus for query relevance in URL
        if self.query.lower() in url.lower():
            priority -= 50
            
        # Bonus for switching domains
        if parent_domain and domain != parent_domain:
            priority -= 30
            
        return priority

    def _add_url_to_queue(self, url, depth, parent_domain=None):
        """
        Internal method to add a URL to the queue.
        MUST be called within the shared_state_lock.
        """
        url_hash = self.get_url_hash(url)
        if url_hash not in self.visited_hashes and depth <= self.max_depth:
            # Add to visited immediately to prevent other workers from adding it again
            self.visited_hashes.add(url_hash) 
            priority = self._calculate_priority(url, depth, parent_domain)
            heapq.heappush(self.url_queue, PriorityURL(url, depth, priority))
            logger.debug(f"Added to queue: {url} (priority: {priority:.0f}, depth: {depth})")
            
    def _log_crawl(self, url, response_size, depth, status):
        """
        Internal method to log crawled URL.
        MUST be called within the shared_state_lock.
        """
        timestamp = datetime.now().isoformat()
        log_entry = f"{timestamp}|{status}|{depth}|{response_size}|{url}\n"
        self.crawl_log_file.write(log_entry)
        self.crawl_log_file.flush()

    def _extract_links(self, html, base_url):
        """Extract all valid http/https links from HTML content."""
        soup = BeautifulSoup(html, 'html.parser')
        links = set() # Use a set for automatic deduplication of links on the same page
        for tag in soup.find_all(['a', 'link'], href=True):
            href = tag.get('href')
            if href:
                full_url = urljoin(base_url, href.strip())
                if full_url.startswith(('http://', 'https://')):
                    links.add(full_url.split('#')[0]) # Remove URL fragments
        return list(links)

    async def fetch_url(self, session, url):
        """
        Fetches a single URL. This method is now lock-free and only performs
        network I/O and parsing. It returns a result dictionary.
        """
        try:
            async with session.get(url, timeout=10, allow_redirects=True) as response:
                content = await response.read()
                result = {
                    'success': True,
                    'status': response.status,
                    'size': len(content),
                    'links': [],
                    'domain': urlparse(str(response.url)).netloc # Use final URL domain after redirects
                }
                # Extract links only if content is HTML
                if 'text/html' in response.headers.get('Content-Type', ''):
                    html = content.decode('utf-8', errors='ignore')
                    result['links'] = self._extract_links(html, str(response.url))
                return result
        except Exception as e:
            logger.warning(f"Error fetching {url}: {type(e).__name__}")
            return {'success': False, 'status': 'ERROR', 'domain': urlparse(url).netloc}

    async def worker(self, session):
        """
        A worker processes URLs from the queue. It manages locks to ensure
        thread-safe access to shared data.
        """
        while True:
            # --- CRITICAL SECTION START ---
            # Acquire lock to safely get a URL from the shared queue
            with self.shared_state_lock:
                if self.pages_crawled >= self.max_pages:
                    break # Stop if max pages reached
                
                if not self.url_queue:
                    # If queue is empty, release lock and wait
                    url_to_fetch, depth = None, None
                else:
                    priority_url = heapq.heappop(self.url_queue)
                    url_to_fetch, depth = priority_url.url, priority_url.depth
            # --- CRITICAL SECTION END ---
            
            if not url_to_fetch:
                await asyncio.sleep(0.5) # Wait for more URLs
                continue

            # --- I/O OPERATION ---
            result = await self.fetch_url(session, url_to_fetch)
            
            # --- CRITICAL SECTION START ---
            # Re-acquire lock to update shared state with the result
            with self.shared_state_lock:
                self.pages_crawled += 1
                domain = result['domain']
                
                # Update domain stats
                self.domain_counts[domain] += 1
                self.domain_last_visit[domain] = time.time()
                
                # Add new found links to the queue
                if result['success'] and depth < self.max_depth:
                    for link in result['links']:
                        self._add_url_to_queue(link, depth + 1, parent_domain=domain)
                
                # Log the crawl result
                self._log_crawl(url_to_fetch, result.get('size', 0), depth, result['status'])

                # Update and print progress safely
                elapsed = time.time() - self.start_time
                rate = self.pages_crawled / elapsed if elapsed > 0 else 0
                print(
                    f"\rCrawled: {self.pages_crawled}/{self.max_pages} | "
                    f"Queue: {len(self.url_queue):,} | "
                    f"Rate: {rate:.2f} pages/s", end=''
                )
            # --- CRITICAL SECTION END ---

    async def crawl_async_manager(self):
        """Manages the async workers for a single thread."""
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10, force_close=True)
        async with aiohttp.ClientSession(
            connector=connector,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; MySimpleCrawler/1.0)'}
        ) as session:
            # Calculate workers per thread, ensuring at least one
            workers_per_thread = max(1, self.concurrent_requests // self.num_threads)
            tasks = [self.worker(session) for _ in range(workers_per_thread)]
            await asyncio.gather(*tasks)

    def thread_entry_point(self):
        """The entry point for each thread created by the ThreadPoolExecutor."""
        asyncio.run(self.crawl_async_manager())

    def crawl(self):
        """Main crawl function that sets up and runs the multithreaded crawler."""
        logger.info(f"Starting crawl for query: '{self.query}' with {self.num_threads} threads.")
        self.start_time = time.time()
        
        with open('crawl_log.csv', 'w') as self.crawl_log_file:
            self.crawl_log_file.write("Timestamp|Status|Depth|Size(bytes)|URL\n")

            initial_urls = [
                f"https://www.google.com/search?q={self.query.replace(' ', '+')}",
                f"https://www.bing.com/search?q={self.query.replace(' ', '+')}",
                f"https://duckduckgo.com/?q={self.query.replace(' ', '+')}",
            ]
            
            # Initial population of the queue must be done under the lock
            with self.shared_state_lock:
                for url in initial_urls:
                    self._add_url_to_queue(url, depth=0)
            
            with ThreadPoolExecutor(max_workers=self.num_threads, thread_name_prefix='CrawlerThread') as executor:
                # Submit one task per configured thread
                futures = [executor.submit(self.thread_entry_point) for _ in range(self.num_threads)]
                for future in futures:
                    try:
                        future.result() # Wait for threads to complete
                    except Exception as e:
                        logger.error(f"A thread raised an exception: {e}")

        # Final statistics
        elapsed = time.time() - self.start_time
        rate = self.pages_crawled / elapsed if elapsed > 0 else 0
        
        print("\n\n--- Crawl Finished ---")
        print(f"Total pages crawled: {self.pages_crawled}")
        print(f"Total time: {elapsed:.2f}s")
        print(f"Average rate: {rate:.2f} pages/s")
        print(f"Unique domains visited: {len(self.domain_counts)}")
        print("\nTop 10 most visited domains:")
        sorted_domains = sorted(self.domain_counts.items(), key=lambda x: x[1], reverse=True)
        for domain, count in sorted_domains[:10]:
            print(f"  - {domain}: {count} pages")

def main():
    query = input("Enter search query: ")
    crawler = WebCrawler(
        query=query, 
        max_pages=5000, 
        max_depth=4, 
        concurrent_requests=50,  
        num_threads=8
    )
    crawler.crawl()

if __name__ == "__main__":
    main()