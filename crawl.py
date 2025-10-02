import time
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from datetime import datetime
import logging
from collections import defaultdict, deque
import heapq
import hashlib
from concurrent.futures import ThreadPoolExecutor
import threading
from urllib.robotparser import RobotFileParser
import queue

# POINT 4: OTHER FILE AVOIDING - This list defines file extensions to ignore before fetching.
# First line of defense against non-HTML type pages
URL_BLACKLIST_EXTENSIONS = [
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg',  # Images
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',  # Documents
    '.zip', '.rar', '.tar', '.gz',  # Archives
    '.css', '.js',  # Assets
    '.mp3', '.mp4', '.avi', '.mov'  # Media
]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawl_log1.log'),
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
    def __init__(self, query, max_pages=500, max_depth=3, num_threads=8):
        self.query = query
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.num_threads = num_threads
        
        # Shared state
        # POINT 5: PAST AVOIDANCE - This set will store hashes of visited URLs to prevent re-crawling.
        self.visited_hashes = set()
        self.url_queue = []
        self.domain_counts = defaultdict(int)
        self.domain_last_visit = defaultdict(float)
        self.pages_crawled = 0
        self.error_pages = 0
        
        # Robot parsers with expiry
        self.robot_parsers = {}
        self.robot_parsers_lock = threading.Lock()
        self.robot_fetch_cache = {}  # Cache for domains we've tried to fetch
        
        self.start_time = None
        
        # Separate locks for better concurrency
        self.queue_lock = threading.Lock()
        self.visited_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        
        # FIX 1: Add shutdown event to signal all threads to stop
        self.shutdown_event = threading.Event()
        
        # Async logging queue
        self.log_queue = queue.Queue(maxsize=10000)
        self.log_thread = None
        self.stop_logging = threading.Event()
        
        # Session pool (one per thread for better performance)
        self.sessions = threading.local()
        
        # Batch processing
        self.pending_urls = deque()
        self.pending_lock = threading.Lock()
        
    def get_session(self):
        """Get thread-local session"""
        if not hasattr(self.sessions, 'session'):
            self.sessions.session = requests.Session()
            self.sessions.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (compatible; AcademicCrawler/1.0)'
            })
            # Connection pooling
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=10,
                pool_maxsize=20,
                max_retries=0
            )
            self.sessions.session.mount('http://', adapter)
            self.sessions.session.mount('https://', adapter)
        return self.sessions.session
    
    def get_url_hash(self, url):
        """Get MD5 hash of URL for efficient storage"""
        return hashlib.md5(url.encode()).hexdigest()
    
    def _calculate_priority(self, url, depth, parent_domain=None):
        """Calculate URL priority (lock-free)"""
        domain = urlparse(url).netloc
        priority = depth * 100
        
        # Use get with defaults to avoid lock
        priority += self.domain_counts.get(domain, 0) * 50
        
        time_since_visit = time.time() - self.domain_last_visit.get(domain, 0)
        if time_since_visit < 5:
            priority += 200
            
        if self.query.lower() in url.lower():
            priority -= 50
            
        if parent_domain and domain != parent_domain:
            priority -= 30
            
        return priority
    
    def _should_skip_url(self, url):
        """Fast URL filtering without locks"""
        # POINT 6: EXCEPTION HANDLING - Catches errors during URL parsing.
        try:
            path = urlparse(url).path
            # POINT 4: OTHER FILE AVOIDING - Checks the URL against the blacklist of file extensions.
            if any(path.lower().endswith(ext) for ext in URL_BLACKLIST_EXTENSIONS):
                return True
            # POINT 1: AVOID CGI SCRIPTS - Explicitly checks for and ignores URLs containing 'cgi'.
            if 'cgi' in url.lower():
                return True
            return False
        except Exception:
            return True
    
    def _add_urls_batch(self, urls_with_depth, parent_domain=None):
        """Add multiple URLs at once - called with queue_lock held"""
        added = 0
        for url, depth in urls_with_depth:
            if self._should_skip_url(url):
                continue
            
            url_hash = self.get_url_hash(url)
            
            # Quick visited check without lock
            with self.visited_lock:
                # POINT 5: PAST AVOIDANCE - Checks if the URL hash is already in the visited set.
                if url_hash in self.visited_hashes:
                    continue
                # POINT 5: PAST AVOIDANCE - Adds the new hash to the set to mark it as visited.
                self.visited_hashes.add(url_hash)
            
            if depth <= self.max_depth:
                priority = self._calculate_priority(url, depth, parent_domain)
                heapq.heappush(self.url_queue, PriorityURL(url, depth, priority))
                added += 1
        
        return added
    
    def _log_async(self, url, response_size, depth, status):
        """Add log entry to queue for async writing"""
        try:
            timestamp = datetime.now().isoformat()
            log_entry = f"{timestamp}|{status}|{depth}|{response_size}|{url}\n"
            self.log_queue.put_nowait(log_entry)
        except queue.Full:
            pass  # Drop logs if queue is full
    
    def log_writer_thread(self):
        """Background thread for writing logs"""
        with open('crawl_log1.txt', 'w') as f:
            f.write("Timestamp|Status|Depth|Size(bytes)|URL\n")
            while not self.stop_logging.is_set() or not self.log_queue.empty():
                try:
                    log_entry = self.log_queue.get(timeout=0.1)
                    f.write(log_entry)
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"Log writing error: {e}")

    def _extract_links(self, html, base_url):
        """Extract all valid http/https links from HTML"""
        # POINT 6: EXCEPTION HANDLING - Catches errors if HTML parsing fails.
        try:
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            for tag in soup.find_all(['a', 'link'], href=True):
                href = tag.get('href')
                if href:
                    # POINT 3: RELATIVE URLS - `urljoin` correctly converts relative URLs to absolute ones.
                    full_url = urljoin(base_url, href.strip())
                    if full_url.startswith(('http://', 'https://')):
                        # POINT 2: AMBIGUOUS HYPERLINKS - Strips URL fragments (#) to prevent duplicate crawling.
                        links.append(full_url.split('#')[0])
            return links
        except Exception as e:
            logger.warning(f"Error extracting links from {base_url}: {e}")
            return []
    
    # POINT 1: ROBOT PARSER - This function is responsible for fetching and parsing robots.txt files.
    def _get_robot_parser(self, domain, scheme='https'):
        """Fetch and cache robots.txt - optimized with better caching"""
        # Check cache first without lock
        if domain in self.robot_parsers:
            return self.robot_parsers[domain]
        
        # Check if we've tried fetching before
        cache_key = f"{scheme}://{domain}"
        with self.robot_parsers_lock:
            if cache_key in self.robot_fetch_cache:
                return self.robot_fetch_cache[cache_key]
        
        # Fetch outside lock
        rp = RobotFileParser()
        robots_url = f"{scheme}://{domain}/robots.txt"
        rp.set_url(robots_url)
        
        # POINT 6: EXCEPTION HANDLING - Catches network errors when fetching robots.txt.
        try:
            # POINT 8: TIMEOUTS - Sets a short timeout for fetching the robots.txt file.
            response = self.get_session().get(robots_url, timeout=0.5)
            if response.status_code == 200:
                from io import StringIO
                rp.parse(StringIO(response.text).readlines())
        except Exception:
            pass  # Allow on error
        
        # Store in cache
        with self.robot_parsers_lock:
            self.robot_fetch_cache[cache_key] = rp
            self.robot_parsers[domain] = rp
        
        return rp

    # POINT 1: ROBOT PARSER - This method uses the parser to check if fetching a URL is allowed.
    def _can_fetch(self, url):
        """Check robots.txt - with fast path"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            if not domain:
                return False
            
            # Fast path: if we've crawled this domain before, it's probably OK
            if self.domain_counts.get(domain, 0) > 0:
                return True
            
            rp = self._get_robot_parser(domain, parsed.scheme)
            return rp.can_fetch('AcademicCrawler/1.0', url)
        except Exception:
            return True
    
    def _is_captcha_or_blocked(self, html, url):
        """Detect CAPTCHA or bot detection"""
        try:
            html_lower = html.lower()
            captcha_indicators = [
                'captcha', 'recaptcha', 'are you a robot',
                'verify you are human', 'unusual traffic',
                'automated requests', 'access denied'
            ]
            
            return any(indicator in html_lower for indicator in captcha_indicators)
        except Exception:
            return False
    
    # POINT 9: AVOID PASSWORD-PROTECTED PAGES - Detects login forms by searching for password fields.
    def _is_login_page(self, html):
        """Detect login pages"""
        try:
            html_lower = html.lower()
            return 'type="password"' in html_lower or 'type=password' in html_lower
        except Exception:
            return False

    def fetch_url(self, url):
        """Fetch a single URL - optimized"""
        # POINT 6: EXCEPTION HANDLING - This broad try/except block makes fetching resilient to any network error.
        try:
            session = self.get_session()
            # POINT 8: TIMEOUTS - Sets a 5-second timeout for the main page request.
            # POINT 3: URL FORWARDING - `allow_redirects=True` handles HTTP redirects automatically.
            response = session.get(url, timeout=5, allow_redirects=True)
            
            # POINT 7: NON-SUCCESS CODES HANDLING - Checks for any status code other than 200 OK.
            if response.status_code != 200:
                if response.status_code == 404:
                    with self.stats_lock:
                        self.error_pages += 1
                return {
                    'success': False,
                    'status': response.status_code,
                    'size': 0,
                    'links': [],
                    'domain': urlparse(response.url).netloc
                }
            
            result = {
                'success': True,
                'status': response.status_code,
                'size': len(response.content),
                'links': [],
                'domain': urlparse(response.url).netloc
            }
            
            # POINT 4: OTHER FILE AVOIDING - Second line of defense by checking the Content-Type header.
            # Extract links only for HTML
            if 'text/html' in response.headers.get('Content-Type', ''):
                if self._is_captcha_or_blocked(response.text, url):
                    result['success'] = False
                    return result
                # POINT 9: AVOID PASSWORD-PROTECTED PAGES - Skips link extraction if a login page is detected.
                if not self._is_login_page(response.text):
                    result['links'] = self._extract_links(response.text, response.url)
            
            return result
        except Exception as e:
            logger.warning(f"Error fetching {url}: {type(e).__name__}")
            return {
                'success': False,
                'status': 'ERROR',
                'domain': urlparse(url).netloc,
                'size': 0,
                'links': []
            }

    def worker(self):
        """Worker thread - optimized for speed"""
        logger.info(f"{threading.current_thread().name} started")
        
        empty_queue_count = 0
        
        try:
            while not self.shutdown_event.is_set():
                # Check if we've reached max pages
                if self.pages_crawled >= self.max_pages:
                    break
                
                # Get URL from queue
                url_to_fetch = None
                depth = None
                
                with self.queue_lock:
                    if self.url_queue:
                        priority_url = heapq.heappop(self.url_queue)
                        url_to_fetch = priority_url.url
                        depth = priority_url.depth
                        empty_queue_count = 0  # Reset counter when we find URLs
                
                if not url_to_fetch:
                    # Check if we should exit (no URLs and reached limit)
                    if self.pages_crawled >= self.max_pages:
                        break
                    
                    # Increment empty queue counter
                    empty_queue_count += 1
                    
                    # If queue has been empty for 50 iterations (5 seconds) and we haven't reached max_pages,
                    # assume crawl is stuck and exit
                    if empty_queue_count > 50:
                        logger.warning(f"{threading.current_thread().name} - Queue empty for too long, exiting")
                        break
                    
                    time.sleep(0.1)
                    continue

                # POINT 1: ROBOT PARSER - Applies the robots.txt check before fetching the URL.
                # Check robots.txt (optimized with fast path)
                if not self._can_fetch(url_to_fetch):
                    continue

                # Fetch the URL
                result = self.fetch_url(url_to_fetch)
                
                # Update stats and check if we've hit the limit
                should_stop = False
                with self.stats_lock:
                    self.pages_crawled += 1
                    current_count = self.pages_crawled
                    domain = result['domain']
                    self.domain_counts[domain] += 1
                    self.domain_last_visit[domain] = time.time()
                    
                    # If we've hit max_pages, signal shutdown
                    if current_count >= self.max_pages:
                        self.shutdown_event.set()
                        should_stop = True
                
                # Async logging
                self._log_async(url_to_fetch, result['size'], depth, result['status'])
                
                # Only add new links if we haven't hit the limit
                if not should_stop and result['success'] and depth < self.max_depth:
                    urls_with_depth = [(link, depth + 1) for link in result['links']]
                    if urls_with_depth:
                        with self.queue_lock:
                            self._add_urls_batch(urls_with_depth, parent_domain=domain)
                
                # Print progress (less frequently)
                if current_count % 10 == 0:
                    elapsed = time.time() - self.start_time
                    rate = current_count / elapsed if elapsed > 0 else 0
                    with self.queue_lock:
                        queue_size = len(self.url_queue)
                    print(
                        f"\rCrawled: {current_count}/{self.max_pages} | "
                        f"Queue: {queue_size:,} | "
                        f"Rate: {rate:.2f} pages/s", end=''
                    )
                
                # Break immediately if we hit max_pages
                if should_stop:
                    break
        
        finally:
            logger.info(f"{threading.current_thread().name} exiting")

    def crawl(self):
        """Main crawl function"""
        logger.info(f"Starting crawl for query: '{self.query}' with {self.num_threads} threads.")
        self.start_time = time.time()
        
        # Start async log writer
        self.log_thread = threading.Thread(target=self.log_writer_thread, daemon=True)
        self.log_thread.start()
        
        executor = None
        try:
            # Add initial URLs
            # for crawl_log1.txt
            initial_urls = [
                "https://www.nasa.gov/",
                "https://www.nsf.gov/", # National Science Foundation
                
                # Web Standards & Directories
                "https://www.w3.org/",
                "https://www.dmoz-odp.org/",
                "https://tools.ietf.org/",
            ]

            with self.queue_lock:
                urls_with_depth = [(url, 0) for url in initial_urls]
                self._add_urls_batch(urls_with_depth)
            
            logger.info(f"Added {len(initial_urls)} initial URLs to queue")
            
            # Start worker threads
            executor = ThreadPoolExecutor(max_workers=self.num_threads, thread_name_prefix='Worker')
            futures = [executor.submit(self.worker) for _ in range(self.num_threads)]
            
            # Monitor progress and force shutdown if needed
            while not self.shutdown_event.is_set():
                time.sleep(0.2)
                if self.pages_crawled >= self.max_pages:
                    logger.info("Max pages reached, signaling shutdown...")
                    self.shutdown_event.set()
                    break
            
            # Give threads a moment to finish current requests
            logger.info("Waiting for threads to complete...")
            time.sleep(1)
            
        finally:
            # Force shutdown
            self.shutdown_event.set()
            
            # Shutdown executor immediately without waiting
            if executor:
                executor.shutdown(wait=False, cancel_futures=True)
            
            # Stop logging thread
            self.stop_logging.set()
            if self.log_thread:
                self.log_thread.join(timeout=5)

        # Final statistics
        elapsed = time.time() - self.start_time
        rate = self.pages_crawled / elapsed if elapsed > 0 else 0
        
        print("\n\n--- Crawl Finished ---")
        print(f"Total pages crawled: {self.pages_crawled}")
        print(f"Total time: {elapsed:.2f}s")
        print(f"Average rate: {rate:.2f} pages/s")
        print(f"Unique domains visited: {len(self.domain_counts)}")
        print(f"Number of 404s faced: {self.error_pages}")
        print("\nTop 10 most visited domains:")

        sorted_domains = sorted(self.domain_counts.items(), key=lambda x: x[1], reverse=True)
        for domain, count in sorted_domains[:10]:
            print(f"  - {domain}: {count} pages")

def main():
    query = input("Enter search query: ")
    max_pages = int(input("Enter Max pages you want to crawl: "))
    max_depth = int(input("Enter Max depth you want to allow: "))
    num_threads = int(input("Enter number of threads you wish to use: "))
    crawler = WebCrawler(
        query=query,
        max_pages=max_pages,
        max_depth=max_depth,
        num_threads=num_threads
    )
    crawler.crawl()

if __name__ == "__main__":
    main()