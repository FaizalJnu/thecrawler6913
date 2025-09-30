import asyncio
import aiohttp
import time
from urllib.parse import urljoin, urlparse, parse_qs
from urllib.robotparser import RobotFileParser
import socket
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
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug.txt'),
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
        
        self.visited = set()
        self.url_queue = []
        self.domain_counts = defaultdict(int) 
        self.domain_last_visit = defaultdict(float)  
        
        self.pages_crawled = 0
        self.start_time = None
        self.crawl_log = None
        self.robot_parser = None
        
        # Thread-safe locks
        self.visited_lock = threading.Lock()
        self.queue_lock = threading.Lock()
        self.counter_lock = threading.Lock()
        self.domain_lock = threading.Lock()
        self.log_lock = threading.Lock()
        
    def get_url_hash(self, url):
        """Get hash of URL for deduplication"""
        return hashlib.md5(url.encode()).hexdigest()
    
    def calculate_priority(self, url, depth, parent_domain=None):
        """
        Calculate priority score for URL. Lower score = higher priority.
        Factors:
        - Depth (lower depth = higher priority)
        - Domain diversity (penalize over-represented domains)
        - Query relevance (bonus for URLs containing query terms)
        - Recency (penalize recently visited domains)
        """
        domain = urlparse(url).netloc
        
        # Base priority on depth
        priority = depth * 100
        
        # Penalize domains we've visited a lot
        with self.domain_lock:
            domain_penalty = self.domain_counts[domain] * 50
            priority += domain_penalty
        
            # Penalize domains we visited recently (encourage domain switching)
            if domain in self.domain_last_visit:
                time_since_visit = time.time() - self.domain_last_visit[domain]
                if time_since_visit < 5:  # Within 5 seconds
                    priority += 200
        
        # Bonus for query relevance in URL
        query_terms = self.query.lower().split()
        url_lower = url.lower()
        relevance_bonus = sum(50 for term in query_terms if term in url_lower)
        priority -= relevance_bonus
        
        # Bonus for different domain than parent
        if parent_domain and domain != parent_domain:
            priority -= 30
        
        return priority
    
    def add_url(self, url, depth, parent_domain=None):
        """Add URL to priority queue (thread-safe)"""
        url_hash = self.get_url_hash(url)
        
        with self.visited_lock:
            already_visited = url_hash in self.visited
        
        if not already_visited and depth <= self.max_depth:
            priority = self.calculate_priority(url, depth, parent_domain)
            with self.queue_lock:
                heapq.heappush(self.url_queue, PriorityURL(url, depth, priority))
            logger.debug(f"Added to queue: {url} (priority: {priority:.0f}, depth: {depth})")
    
    def log_crawl(self, url, response_size, depth, status, sampled=True):
        """Log crawled URL to file (thread-safe)"""
        timestamp = datetime.now().isoformat()
        log_entry = f"URL: {url} | Time: {timestamp} | Size: {response_size} bytes | Depth: {depth} | Status: {status} | Sampled: {sampled}\n"
        with self.log_lock:
            self.crawl_log.write(log_entry)
            self.crawl_log.flush()
    
    def extract_links(self, html, base_url):
        """Extract all links from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        
        for tag in soup.find_all(['a', 'link'], href=True):
            href = tag.get('href')
            if href:
                full_url = urljoin(base_url, href)
                # Filter out non-http(s) links
                if full_url.startswith(('http://', 'https://')):
                    # Remove fragments
                    full_url = full_url.split('#')[0]
                    links.append(full_url)
        
        return links
    
    async def fetch_url(self, session, url, depth):
        """Fetch a single URL"""
        domain = urlparse(url).netloc
        
        try:
            async with session.get(
                url, 
                timeout=aiohttp.ClientTimeout(total=10),
                allow_redirects=True
            ) as response:
                # Update domain tracking
                with self.domain_lock:
                    self.domain_counts[domain] += 1
                    self.domain_last_visit[domain] = time.time()
                
                content = await response.read()
                content_size = len(content)
                
                # Log the crawl
                self.log_crawl(url, content_size, depth, response.status)
                
                # Extract links if it's HTML
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' in content_type and depth < self.max_depth:
                    html = content.decode('utf-8', errors='ignore')
                    links = self.extract_links(html, url)
                    
                    # Add links to queue with current domain as parent
                    for link in links:
                        self.add_url(link, depth + 1, domain)
                    
                    logger.debug(f"Extracted {len(links)} links from {url}")
                
                return True
                
        except asyncio.TimeoutError:
            logger.warning(f"Timeout: {url}")
            self.log_crawl(url, 0, depth, 'TIMEOUT')
        except Exception as e:
            logger.warning(f"Error fetching {url}: {str(e)}")
            self.log_crawl(url, 0, depth, 'ERROR')
        
        return False
    
    async def worker(self, session):
        """Worker coroutine to process URLs from queue"""
        while True:
            with self.counter_lock:
                if self.pages_crawled >= self.max_pages:
                    break
            
            priority_url = None
            with self.queue_lock:
                if self.url_queue:
                    priority_url = heapq.heappop(self.url_queue)
            
            if priority_url is None:
                await asyncio.sleep(0.1)  # Wait for more URLs
                continue
            
            url = priority_url.url
            depth = priority_url.depth
            url_hash = self.get_url_hash(url)
            
            with self.visited_lock:
                if url_hash in self.visited:
                    continue
                self.visited.add(url_hash)
            
            await self.fetch_url(session, url, depth)
            
            with self.counter_lock:
                self.pages_crawled += 1
                pages = self.pages_crawled
            
            # Display crawl rate
            elapsed = time.time() - self.start_time
            rate = pages / elapsed if elapsed > 0 else 0
            
            with self.queue_lock:
                queue_size = len(self.url_queue)
            
            with self.domain_lock:
                num_domains = len(self.domain_counts)
            
            print(f"\rPages: {pages}/{self.max_pages} | Rate: {rate:.2f} pages/s | Queue: {queue_size} | Domains: {num_domains}", end='')
    
    async def crawl_async(self):
        """Async crawl function for a single thread"""
        # Create aiohttp session
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; WebCrawler/1.0)'}
        ) as session:
            # Create worker tasks
            workers_per_thread = self.concurrent_requests // self.num_threads
            workers = [asyncio.create_task(self.worker(session)) for _ in range(workers_per_thread)]
            
            # Wait for all workers to complete
            await asyncio.gather(*workers)
    
    def thread_crawl(self):
        """Entry point for each thread"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.crawl_async())
        finally:
            loop.close()
    
    def crawl(self):
        """Main crawl function with multithreading"""
        logger.info(f"Starting crawl with query: '{self.query}' using {self.num_threads} threads")
        self.start_time = time.time()
        self.crawl_log = open('crawl1.log', 'w')
        
        # Initialize with search URLs based on query
        initial_urls = [
            f"https://www.google.com/search?q={self.query.replace(' ', '+')}",
            f"https://www.bing.com/search?q={self.query.replace(' ', '+')}",
            f"https://duckduckgo.com/?q={self.query.replace(' ', '+')}",
        ]
        
        for url in initial_urls:
            self.add_url(url, 0)
        
        try:
            # Create thread pool
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                futures = [executor.submit(self.thread_crawl) for _ in range(self.num_threads)]
                
                # Wait for all threads to complete
                for future in futures:
                    future.result()
            
        finally:
            self.crawl_log.close()
        
        # Final statistics
        elapsed = time.time() - self.start_time
        rate = self.pages_crawled / elapsed if elapsed > 0 else 0
        
        print(f"\n\nCrawl completed!")
        print(f"Total pages crawled: {self.pages_crawled}")
        print(f"Total time: {elapsed:.2f}s")
        print(f"Average rate: {rate:.2f} pages/s")
        print(f"Unique domains: {len(self.domain_counts)}")
        print(f"\nTop 10 domains:")
        for domain, count in sorted(self.domain_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {domain}: {count} pages")

def main():
    query = input("Enter search query: ")
    crawler = WebCrawler(
        query=query, 
        max_pages=5000, 
        max_depth=3, 
        concurrent_requests=40,  
        num_threads=10
    )
    crawler.crawl()

if __name__ == "__main__":
    main()