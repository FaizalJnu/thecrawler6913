import heapq
import threading
import requests
import html.parser
from datetime import datetime
import urllib.parse
import random
from concurrent.futures import ThreadPoolExecutor
import logging
import re
import urllib.robotparser
from urllib.parse import urljoin, urlparse
import time
from bs4 import BeautifulSoup


start_time = time.time()    

class LinkExtractor: 
    """
    Extracts absolute links from HTML content using BeautifulSoup for robustness.
    Keeps the feed() and get_links() interface for minimal changes in Crawler.
    """
    def __init__(self, base_url):
        self.base_url = base_url
        self.links = []

    def feed(self, html_content):
        """Parses the HTML content and extracts all links."""
        try:
            # Use BeautifulSoup to parse the HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find all <a> tags
            for link_tag in soup.find_all('a'):
                href = link_tag.get('href')
                if href:
                    # Use urljoin to correctly resolve relative URLs to absolute URLs
                    absolute_url = urllib.parse.urljoin(self.base_url, href)
                    self.links.append(absolute_url)
        except Exception:
             # Fail gracefully if parsing fails
            pass
    
    def get_links(self):
        return self.links
    

class ScoredPriorityQueue:
    """
    A faster, simplified priority queue that promotes breadth-first crawling
    and domain diversity with minimal computation.
    
    Priority is determined primarily by crawl depth (lower is better). A hash of
    the domain is used as a secondary tie-breaker to interleave different
    domains at the same depth level.
    """
    def __init__(self):
        self._queue = []
        self._index = 0  # Tie-breaker for items with the exact same priority
        self._lock = threading.Lock()
        self._url_depths = {} # Tracks the shallowest depth a URL has been seen at

    def normalize_url(self, url):
        """
        Normalizes a URL by removing the fragment and ensuring a consistent format.
        """
        parsed = urlparse(url)
        normalized = parsed._replace(fragment='').geturl()
        return normalized.lower().strip('/')

    def put(self, url, depth=0):
        """
        Adds a URL to the queue. The priority score is a tuple of (depth, domain_hash),
        which is extremely fast to calculate.
        """
        norm_url = self.normalize_url(url)
        with self._lock:
            #! Skipping if e already enqueued this URL at an equal or shallower depth
            if norm_url in self._url_depths and depth >= self._url_depths[norm_url]:
                return

            domain = urlparse(url).netloc
            priority = (depth, hash(domain))

            self._url_depths[norm_url] = depth
            heapq.heappush(self._queue, (priority, self._index, url, depth))
            self._index += 1

    def get(self, timeout=None):
        """
        Retrieves the highest-priority (lowest score) URL from the queue.
        """
        with self._lock:
            if not self._queue:
                raise Exception("Queue is empty")
            
            # Item structure: ( (depth, domain_hash), index, url, depth )
            _priority, _index, url, depth = heapq.heappop(self._queue)
            return url, depth

    def empty(self):
        with self._lock:
            return len(self._queue) == 0

    def task_done(self):
        """Compatibility method for queue.Queue interface."""
        pass

    def qsize(self):
        with self._lock:
            return len(self._queue)


class RobotRules:
    """
    A comprehensive robots.txt handler that respects crawl delays,
    user agent restrictions, and caches rules per domain.
    """
    
    def __init__(self, user_agent="*", default_crawl_delay=0): #! adjust default crawl delay based on speed and politness
        self.user_agent = user_agent
        self.default_crawl_delay = default_crawl_delay
        self.robots_cache = {}  # Domain -> RobotFileParser
        self.crawl_delays = {}  # Domain -> crawl delay in seconds
        self.last_access_time = {}  # Domain -> last access  
        self.fetch_lock = threading.Lock()  # Thread safety for robots.txt fetching
        self.access_locks = {}  # Per-domain locks for rate limiting
        
        # Set up logging
        self.logger = logging.getLogger('robots_logger')
        
    def get_domain(self, url):
        """Extract domain from URL for caching purposes."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    def fetch_robots_txt(self, domain):
        """
        Fetch and parse robots.txt for a given domain.
        Returns True if successfully fetched, False otherwise.
        """
        robots_url = urllib.parse.urljoin(domain, '/robots.txt')
        
        try:
            # Create a RobotFileParser instance
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(robots_url)
            
            # Try to read the robots.txt file
            response = requests.get(robots_url, timeout=10, 
                                  headers={'User-Agent': self.user_agent})
            
            if response.status_code == 200:
                # Parse the robots.txt content
                rp.set_url(robots_url)
                robots_content = response.text
                rp.parse(robots_content) 
                
                # some caching to improve speed
                self.robots_cache[domain] = rp
                
                # trying here to extract crawl delay 
                crawl_delay = self.extract_crawl_delay(robots_content)
                self.crawl_delays[domain] = crawl_delay
                
                # Creating a per-domain lock for rate limiting
                self.access_locks[domain] = threading.Lock()
                
                self.logger.info(f"Successfully fetched robots.txt for {domain} (crawl delay: {crawl_delay}s)")
                return True
                
            elif response.status_code == 404:
                # No robots.txt found, will go ahead allowing all
                rp.set_url(robots_url)
                rp.parse("")  # Empty robots.txt allows everything
                self.robots_cache[domain] = rp
                self.crawl_delays[domain] = self.default_crawl_delay
                self.access_locks[domain] = threading.Lock()
                
                self.logger.info(f"No robots.txt found for {domain}, allowing all URLs")
                return True
                
            else:
                self.logger.warning(f"Failed to fetch robots.txt for {domain}: HTTP {response.status_code}")
                return False
                
        except requests.RequestException as e:
            # self.logger.error(f"Error fetching robots.txt for {domain}: {e}")
            return False
        except Exception as e:
            # self.logger.error(f"Unexpected error parsing robots.txt for {domain}: {e}")
            return False
    
    def extract_crawl_delay(self, robots_content):
        """
        Extract crawl delay from robots.txt content.
        Looks for both 'Crawl-delay' and 'Request-rate' directives.
        """
        lines = robots_content.split('\n')
        current_user_agent = None
        crawl_delay = self.default_crawl_delay
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
                
            if line.lower().startswith('user-agent:'):
                current_user_agent = line.split(':', 1)[1].strip()
                continue
                
            # Check if this rule applies to our user agent
            if current_user_agent and not self.matches_user_agent(current_user_agent):
                continue
                
            # Look for crawl delay directive
            if line.lower().startswith('crawl-delay:'):
                try:
                    delay_value = float(line.split(':', 1)[1].strip())
                    crawl_delay = delay_value
                    break
                except ValueError:
                    continue
                    
            # Look for request rate directive (requests per second)
            elif line.lower().startswith('request-rate:'):
                try:
                    rate_parts = line.split(':', 1)[1].strip().split('/')
                    if len(rate_parts) == 2:
                        requests_num = float(rate_parts[0])
                        time_period = float(rate_parts[1])
                        if requests_num > 0:
                            crawl_delay = time_period / requests_num
                            break
                except ValueError:
                    continue
        
        return crawl_delay
    
    def matches_user_agent(self, robots_user_agent):
        """Check if the robots.txt user agent matches our crawler."""
        robots_ua = robots_user_agent.lower()
        our_ua = self.user_agent.lower()

        if robots_ua == '*':
            return True
            
        return robots_ua == our_ua or robots_ua in our_ua or our_ua in robots_ua
    
    def can_fetch(self, url):
        """
        Checking we're allowed to fetch the given URL according to robots.txt rules.
        Also enforces crawl delay.
        """
        domain = self.get_domain(url)
        
        # Ensure we have robots.txt rules for this domain
        with self.fetch_lock:
            if domain not in self.robots_cache:
                if not self.fetch_robots_txt(domain):
                    # If we can't fetch robots.txt, be conservative and allow
                    self.logger.warning(f"Couldn't fetch robots.txt for {domain}, proceeding with caution")
                    return True
        
        # Check robots.txt permission
        rp = self.robots_cache[domain]
        if not rp.can_fetch(self.user_agent, url):
            self.logger.info(f"Robots.txt disallows fetching: {url}")
            return False
        
        # Enforce crawl delay
        self.enforce_crawl_delay(domain)
        
        return True
    
    def enforce_crawl_delay(self, domain):
        """
        Enforce crawl delay for the given domain.
        Blocks the current thread if necessary.
        """
        if domain not in self.access_locks:
            return
            
        with self.access_locks[domain]:
            current_time = time.time()
            crawl_delay = self.crawl_delays.get(domain, self.default_crawl_delay)
            
            if domain in self.last_access_time:
                time_since_last = current_time - self.last_access_time[domain]
                
                if time_since_last < crawl_delay:
                    sleep_time = crawl_delay - time_since_last
                    self.logger.debug(f"Enforcing crawl delay for {domain}: sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)
            
            self.last_access_time[domain] = time.time()


class Crawler:

    def __init__(self, seed_urls, max_threads, crawl_limit):
        # The thread-safe priority queue to hold URLs to crawl.
        self.url_queue = ScoredPriorityQueue()

        # A set to keep track of visited URLs for deduplication.
        self.visited_urls = set()

        # Initialize robot rules with proper user agent
        self.robot_rules = RobotRules(
            user_agent="DiversityCrawler/1.0 (+http://example.com/bot.html)",
            default_crawl_delay=0.2
        )

        # The thread pool for parallel fetching.
        self.executor = ThreadPoolExecutor(max_workers=max_threads)

        # The maximum number of pages to crawl for the run.
        self.crawl_limit = crawl_limit
        # A counter for the number of pages successfully crawled.
        self.crawled_count = 0

        # Add seed URLs to the queue and visited set.
        for url in seed_urls:
            self.url_queue.put(url, depth=0)
            self.visited_urls.add(self.url_queue.normalize_url(url))

        #! skipping possible urls or computation traps
        self.skip_extensions = (
            '.jpg', '.jpeg', '.png', '.gif', '.pdf', '.zip', '.mp4', '.mp3',
            '.docx', '.xls', '.ppt', '.exe', '.rar'
        )
        self.visited_lock = threading.Lock() 
    
    def run(self, queue_log_handler, success_log_handler):
        """
        The main method to start the crawling process.
        Accepts specific log handlers for different output types.
        """
        self.queue_logger = logging.getLogger('queue_logger')
        self.queue_logger.addHandler(queue_log_handler)
        self.queue_logger.setLevel(logging.INFO)
        self.queue_logger.info("Starting the crawling process...")
        
        self.success_logger = logging.getLogger('success_logger')
        self.success_logger.addHandler(success_log_handler)
        self.success_logger.setLevel(logging.INFO)
        self.success_logger.info("Starting a new crawl run.")

        consecutive_empty_checks = 0
        max_empty_checks = 10  
        
        while self.crawled_count < self.crawl_limit:
            try:
                url, depth = self.url_queue.get(timeout=2)
                consecutive_empty_checks = 0  # Reset counter on successful get
                
                self.queue_logger.info(f"Queue size: {self.url_queue.qsize()}, Crawled: {self.crawled_count}/{self.crawl_limit}, Processing: {url}")

                # Submit the URL for crawling to a worker thread.
                self.executor.submit(self.crawl_page, url, depth)

                # Mark the URL as a completed task.
                self.url_queue.task_done()
                
            except Exception as e:
                consecutive_empty_checks += 1
                self.queue_logger.info(f"Queue timeout/empty (attempt {consecutive_empty_checks}/{max_empty_checks}): {e}")
                
                # If queue has been empty for too long and we have active threads, wait a bit more
                if consecutive_empty_checks >= max_empty_checks:
                    # Check if there are still active worker threads that might add URLs
                    active_threads = sum(1 for t in threading.enumerate() if t.name.startswith('ThreadPoolExecutor'))
                    if active_threads > 1:  # Main thread + at least one worker
                        self.queue_logger.info(f"Still have {active_threads-1} worker threads active, waiting longer...")
                        consecutive_empty_checks = 0  # Reset and wait more
                        time.sleep(5)  # Wait for workers to potentially add more URLs
                        continue
                    else:
                        self.queue_logger.info("No more active workers and queue empty, stopping...")
                        break
                
                # Short wait before retrying
                time.sleep(1)

        self.executor.shutdown(wait=True)
        self.queue_logger.info(f"Crawling completed. Crawled {self.crawled_count} pages.")
        self.success_logger.info(f"Crawling completed. Crawled {self.crawled_count} pages.")

    
    def crawl_page(self, url, depth): 
        """
        The worker function that a thread will execute.
        This is where you will add your robots.txt and prioritization logic.
        """
        self.queue_logger.info(f"ENTERING crawl_page for: {url}")
        try:
            start_time = time.time()
            
            if url.lower().endswith(self.skip_extensions):
                self.queue_logger.info(f"skipping (due to extension): {url}")
                return

            #! figuring out a bug: Check robots.txt before fetching
            if not self.robot_rules.can_fetch(url):
                self.queue_logger.info(f"Robots.txt disallows crawling: {url}")
                return
            
            response = requests.get(url, timeout=5)
            
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' not in content_type:
                self.queue_logger.info(f"skipping (not HTML): {url} with content type {content_type}")
                return
            
            # end_time = time.time()
            # crawl_time = end_time - start_time
            self.queue_logger.info(f"robot.txt allows crawling: {url}")

            crawl_time = datetime.now().isoformat()
            page_size = len(response.content)
            status_code = response.status_code
            
            # Simple sampling logic (10% chance)
            is_sampled = random.random() < 1
            # is_sampled = (self.crawled_count % 10 == 0)

            # Only increment the counter if the crawl was successful
            self.crawled_count += 1
            
            if status_code == 200:
                self.success_logger.info(f"URL: {url} | Time: {crawl_time} | Size: {page_size} bytes | Depth: {depth} | Status: {status_code} | Sampled: {is_sampled}")
                # Use the new LinkExtractor class to get links
                new_links = self.extract_links(response.text, url)

                next_depth = depth + 1
                for link in new_links:
                    # The priority queue will automatically handle prioritization
                    self.add_to_queue(link, next_depth)
            else:
                self.queue_logger.warning(f"Failed to retrieve {url}: Status code {status_code}")

        except requests.exceptions.RequestException as e:
            self.queue_logger.error(f"Request error crawling {url}: {e}")
        except Exception as e:
            self.queue_logger.error(f"Unexpected error crawling {url}: {e}")
            import traceback
            self.queue_logger.error(traceback.format_exc())

    def extract_links(self, html_content, base_url):
        """
        A robust parsing function using a custom HTMLParser.
        """
        parser = LinkExtractor(base_url)
        parser.feed(html_content)
        return parser.get_links()
    
    def add_to_queue(self, url, depth):
        #! Normalize URL for duplicate checking (removes anchors)
        normalized_url = self.url_queue.normalize_url(url)
        
        with threading.Lock():
            if normalized_url not in self.visited_urls:
                self.visited_urls.add(normalized_url)
                self.url_queue.put(normalized_url, depth)  #! Add original URL (with anchor if present)
                self.queue_logger.info(f"Added to queue: {normalized_url}")

if __name__ == "__main__":
    # Disable all console output by setting root logger to CRITICAL
    # This ensures no logging messages appear in the terminal
    logging.getLogger().setLevel(logging.CRITICAL)
    
    # Configure the loggers for the first run
    queue_log1 = logging.FileHandler("debug1.txt", mode='w')
    queue_log1.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'))
    
    success_log1 = logging.FileHandler("crawl1.txt", mode='w')
    success_log1.setFormatter(logging.Formatter('URL: %(message)s'))

    seed_urls_1 = ["https://stackoverflow.com/questions/61847663/matplotlib-backends-backend-tkagg-is-throwing-an-attribute-error-for-blit"]
    my_crawler_1 = Crawler(seed_urls_1, max_threads=24, crawl_limit=500) 
    my_crawler_1.run(queue_log_handler=queue_log1, success_log_handler=success_log1)
    
    logging.getLogger('success_logger').removeHandler(success_log1)

    end_time = time.time()
    # Removed the logging.info statement that could appear in terminal