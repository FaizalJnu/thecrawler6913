# requirements.txt
# duckduckgo-search
# beautifulsoup4

import csv
import math
import queue
import socket
import threading
import time
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from urllib.request import Request, urlopen
from collections import defaultdict

from bs4 import BeautifulSoup
from ddgs import DDGS

# --- Configuration ---
USER_AGENT = "DomainDiversityCrawler/1.0"
MAX_THREADS = 36
MAX_PAGES_TO_CRAWL = 5000
REQUEST_TIMEOUT = 1
URL_BLACKLIST_EXTENSIONS = [
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg',  # Images
    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',  # Documents
    '.zip', '.rar', '.tar', '.gz',  # Archives
    '.css', '.js',  # Assets
    '.mp3', '.mp4', '.avi', '.mov'  # Media
]
LOG_FILE = 'crawl_log.csv'


class DomainDiversityCrawler:
    """
    A multi-threaded web crawler that prioritizes exploring new domains. It uses a
    priority queue to manage its frontier, favoring URLs from domains that have
    been visited less frequently.
    """

    def __init__(self, max_pages, query):
        self.max_pages = max_pages
        # Use a thread-safe PriorityQueue for the URL frontier
        self.frontier = queue.PriorityQueue()
        self.query = query

        # --- Shared data structures with corresponding locks for thread safety ---
        self.visited_urls = set()
        self.visited_lock = threading.Lock()

        self.domain_visit_counts = {}
        self.domain_counts = defaultdict(int)
        self.domain_lock = threading.Lock()
        self.domain_counts_lock = threading.Lock()
        self.domain_last_visit = defaultdict(float)

        self.robot_parsers = {}
        self.robot_parsers_lock = threading.Lock()

        self.log_lock = threading.Lock()

        self.pages_crawled = 0
        self.pages_crawled_lock = threading.Lock()

        # --- Setup logging ---
        try:
            self.log_file = open(LOG_FILE, 'w', newline='', encoding='utf-8')
            self.csv_writer = csv.writer(self.log_file)
            self.csv_writer.writerow([
                'URL', 'Page_Size_Bytes', 'Access_Timestamp', 'HTTP_Status_Code',
                'Priority_Score', 'Crawl_Depth'
            ])
        except IOError as e:
            print(f"Error: Could not open log file {LOG_FILE} for writing: {e}")
            raise

    ##
    ## Core Logic & Helper Functions
    ##---------------------------------------------------------------------------

    def _get_domain(self, url):
        """Extracts the domain (netloc) from a URL."""
        return urlparse(url).netloc

    def _normalize_url(self, url):
        """Normalizes a URL by removing the fragment identifier."""
        parsed = urlparse(url)
        return parsed._replace(fragment='').geturl()

    def _calculate_priority(self, url):
        """
        Calculates URL priority based on its domain's visit count. A lower score
        represents a higher priority. The formula ensures that as a domain is
        visited more, its priority decreases logarithmically.
        """
        domain = self._get_domain(url)
        with self.domain_counts_lock:
            count = self.domain_visit_counts.get(domain, 0)
        
        # The '+ 2' ensures the argument to log is always > 1, avoiding log(1)=0.
        priority = 1 / math.log(count + 2)
        return priority
    
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

    def _log_page(self, url, size, status, priority, depth):
        """Thread-safe logging to the CSV file."""
        timestamp = datetime.utcnow().isoformat()
        with self.log_lock:
            self.csv_writer.writerow([url, size, timestamp, status, priority, depth])
            self.log_file.flush() 

    ##
    ## Network & Parsing Functions
    ##---------------------------------------------------------------------------

    def _fetch_page(self, url):
        """
        Fetches a web page, handling redirects, timeouts, and common HTTP errors.
        It checks the Content-Type header to ensure it's HTML before downloading.
        """
        try:
            req = Request(url, headers={'User-Agent': USER_AGENT})
            with urlopen(req, timeout=REQUEST_TIMEOUT) as response:
                final_url = response.geturl()
                status_code = response.getcode()

                content_type = response.info().get('Content-Type', '')
                if 'text/html' not in content_type:
                    return None, final_url, status_code, 0, "Non-HTML content"

                html_content = response.read()
                page_size = len(html_content)
                return html_content, final_url, status_code, page_size, None
        except HTTPError as e:
            if e.code in [403, 404]:
                print(f"[ERROR] HTTP {e.code} for {url}. Skipping.")
                return None, url, e.code, 0, f"HTTP Error {e.code}"
            return None, url, e.code, 0, str(e)
        except (URLError, socket.timeout, ConnectionResetError) as e:
            print(f"[ERROR] URL or Network error for {url}: {e}. Skipping.")
            return None, url, -1, 0, str(e)
        except Exception as e:
            print(f"[ERROR] An unexpected error occurred for {url}: {e}. Skipping.")
            return None, url, -1, 0, str(e)

    def _parse_links(self, html_content, base_url):
        """
        Parses HTML content to find all hyperlinks, resolving them to absolute URLs.
        Crucially, it respects the HTML <base> tag if present.
        """
        links = set()
        soup = BeautifulSoup(html_content, 'html.parser')
        
        base_tag = soup.find('base', href=True)
        effective_base_url = base_tag['href'] if base_tag else base_url

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].strip()
            if href and not href.startswith('#'):
                absolute_url = urljoin(effective_base_url, href)
                links.add(self._normalize_url(absolute_url))
        return links

    ##
    ## Politeness & Filtering Functions
    ##---------------------------------------------------------------------------
    
    def _get_robot_parser(self, domain):
        """
        Fetches, parses, and caches a robots.txt file for a given domain.
        """
        with self.robot_parsers_lock:
            if domain in self.robot_parsers:
                return self.robot_parsers[domain]

            rp = RobotFileParser()
            robots_url = f"http://{domain}/robots.txt"
            rp.set_url(robots_url)
            
            original_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(REQUEST_TIMEOUT)
            
            try:
                rp.read()
                print(f"[ROBOTS] Fetched and parsed robots.txt for {domain}")
            except Exception as e:
                print(f"[ROBOTS] Could not fetch robots.txt for {domain}: {e}")
            finally:
                socket.setdefaulttimeout(original_timeout)
            
            self.robot_parsers[domain] = rp
            return rp

    def _can_fetch(self, url):
        """Checks if the crawler is allowed to fetch a URL according to robots.txt."""
        domain = self._get_domain(url)
        if not domain:
            return False
        
        rp = self._get_robot_parser(domain)
        return rp.can_fetch(USER_AGENT, url)

    def process_url(self, priority, depth, url):
        """
        The main processing logic for a single URL. This method is executed by a
        worker thread for each URL pulled from the frontier.
        """
        html, final_url, status, size, error = self._fetch_page(url)
        
        final_url_normalized = self._normalize_url(final_url)
        with self.visited_lock:
            self.visited_urls.add(final_url_normalized)

        if error or not html:
            return

        print(f"Crawled: {final_url_normalized} (Depth: {depth}, Priority: {priority:.4f})")
        self._log_page(final_url_normalized, size, status, priority, depth)
        
        domain = self._get_domain(final_url_normalized)
        with self.domain_counts_lock:
            self.domain_visit_counts[domain] = self.domain_visit_counts.get(domain, 0) + 1

        links = self._parse_links(html, final_url_normalized)
        
        for link in links:
            if any(link.lower().endswith(ext) for ext in URL_BLACKLIST_EXTENSIONS):
                continue
            
            with self.visited_lock:
                if link in self.visited_urls:
                    continue
                self.visited_urls.add(link)

            if not self._can_fetch(link):
                print(f"[ROBOTS] Disallowed: {link}")
                continue
            
            new_priority = self.calculate_priority(link, depth+1, parent_domain=domain)
            self.frontier.put((new_priority, depth + 1, link))

    def worker(self):
        """
        The target function for each worker thread. It continuously pulls URLs
        from the frontier and processes them until a stop condition is met.
        """
        while True:
            with self.pages_crawled_lock:
                if self.pages_crawled >= self.max_pages:
                    break
            try:
                priority, depth, url = self.frontier.get(timeout=1)
                
                with self.pages_crawled_lock:
                    if self.pages_crawled < self.max_pages:
                        self.pages_crawled += 1
                    else:
                        self.frontier.task_done()
                        break
                
                self.process_url(priority, depth, url)
                self.frontier.task_done()
            except queue.Empty:
                time.sleep(0.1)
                continue
            except Exception as e:
                print(f"!!! Critical worker error: {e}")

    def run(self, query):
        """Seeds the crawler using DuckDuckGo search and starts the worker threads."""
        print(f"Seeding crawler with top 10 results from DuckDuckGo for query: '{query}'")
        
        # --- MODIFIED SECTION ---
        # This block now uses the DDGS class to get seed URLs.
        try:
            seed_urls = []
            with DDGS() as ddgs:
                # ddgs.text returns a generator of search results
                results = ddgs.text(query, max_results=10)
                # Extract the URL from each result dictionary
                for r in results:
                    seed_urls.append(r['href'])
            
            if not seed_urls:
                print("Could not retrieve any seed URLs from DuckDuckGo. The query might be too specific or there was a network issue.")
                self.cleanup()
                return
        except Exception as e:
            print(f"An error occurred while fetching seed URLs from DuckDuckGo: {e}")
            self.cleanup()
            return

        for url in seed_urls:
            url = self._normalize_url(url)
            with self.visited_lock:
                if url not in self.visited_urls:
                    self.visited_urls.add(url)
                    priority = self.calculate_priority(url, depth=0, parent_domain=None)
                    self.frontier.put((priority, 0, url))

        print(f"--- Starting crawl for max {self.max_pages} pages using {MAX_THREADS} threads ---")
        threads = [threading.Thread(target=self.worker) for _ in range(MAX_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        print("\n--- Crawl finished ---")
        self.cleanup()

    def cleanup(self):
        """Closes any open resources, like the log file."""
        if self.log_file and not self.log_file.closed:
            self.log_file.close()

def main():
    """Main function to run the crawler."""
    crawler = None
    try:
        query = input("Enter your search query to begin crawling: ")
        if not query.strip():
            print("Query cannot be empty.")
            return
            
        crawler = DomainDiversityCrawler(max_pages=MAX_PAGES_TO_CRAWL, query=query)
        crawler.run(query)

    except KeyboardInterrupt:
        print("\nCrawler interrupted by user. Shutting down.")
    except Exception as e:
        print(f"An unexpected error occurred in main: {e}")
    finally:
        if crawler:
            crawler.cleanup()
        print(f"Log file saved as {LOG_FILE}")

if __name__ == "__main__":
    main()