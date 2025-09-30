import urllib.request
import urllib.parse
import urllib.error
import urllib.robotparser
from html.parser import HTMLParser
from collections import deque
import time
import math
import re
from datetime import datetime
import json
import threading
from queue import PriorityQueue, Empty
import heapq

class LinkExtractor(HTMLParser):
    """Extract links from HTML content"""
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
        self.links = []
        self.base_href = None
        
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        
        # Handle base tag
        if tag == 'base' and 'href' in attrs_dict:
            self.base_href = attrs_dict['href']
        
        # Extract href from anchor tags
        if tag == 'a' and 'href' in attrs_dict:
            self.links.append(attrs_dict['href'])
        
        # Extract src from area tags (image maps)
        if tag == 'area' and 'href' in attrs_dict:
            self.links.append(attrs_dict['href'])

class WebCrawler:
    def __init__(self, query, max_pages, timeout, num_threads):
        self.query = query
        self.max_pages = max_pages
        self.timeout = timeout
        self.num_threads = num_threads
        
        # Thread-safe data structures
        self.visited = {}  # URL -> visit info
        self.visited_lock = threading.Lock()
        
        self.domain_counts = {}  # domain -> count
        self.superdomain_counts = {}  # superdomain -> count
        self.domain_lock = threading.Lock()
        
        self.queue = PriorityQueue()  # Thread-safe priority queue
        self.robot_parsers = {}  # domain -> RobotFileParser
        self.robot_lock = threading.Lock()
        
        self.log_file = None
        self.log_lock = threading.Lock()
        
        # Crawl state
        self.pages_crawled = 0
        self.crawl_lock = threading.Lock()
        self.stop_crawling = False
        
        # Domain politeness - track last access time per domain
        self.domain_last_access = {}
        self.domain_access_lock = threading.Lock()
        self.politeness_delay = 0.001  # seconds between requests to same domain
        
        # Blacklist of file extensions to avoid
        self.blacklist_extensions = {
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.ico',
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.zip', '.rar', '.tar', '.gz', '.mp3', '.mp4', '.avi', '.mov',
            '.exe', '.dmg', '.css', '.js', '.xml', '.json', '.rss'
        }
        
    def normalize_url(self, url):
        """Normalize URL by removing fragments and common index files"""
        # Remove fragment
        url = url.split('#')[0]
        
        # Parse URL
        parsed = urllib.parse.urlparse(url)
        
        # Remove default index files
        path = parsed.path
        for index in ['/index.html', '/index.htm', '/index.jsp', '/main.html']:
            if path.endswith(index):
                path = path[:-len(index)] + '/'
                break
        
        # Reconstruct URL
        normalized = urllib.parse.urlunparse((
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            path,
            parsed.params,
            parsed.query,
            ''
        ))
        
        return normalized
    
    def get_domain(self, url):
        """Extract domain from URL"""
        parsed = urllib.parse.urlparse(url)
        return parsed.netloc.lower()
    
    def get_superdomain(self, domain):
        """Extract superdomain (e.g., 'example.com' from 'www.example.com')"""
        parts = domain.split('.')
        if len(parts) >= 2:
            return '.'.join(parts[-2:])
        return domain
    
    def should_crawl(self, url):
        """Check if URL should be crawled based on various criteria"""
        # Check for CGI scripts
        if 'cgi' in url.lower():
            return False
        
        # Check file extension blacklist
        parsed = urllib.parse.urlparse(url)
        path_lower = parsed.path.lower()
        for ext in self.blacklist_extensions:
            if path_lower.endswith(ext):
                return False
        
        # Check scheme
        if parsed.scheme not in ['http', 'https']:
            return False
        
        return True
    
    def check_robots_txt(self, url):
        """Check if URL is allowed by robots.txt (thread-safe)"""
        domain = self.get_domain(url)
        
        with self.robot_lock:
            if domain not in self.robot_parsers:
                parsed = urllib.parse.urlparse(url)
                robots_url = f"{parsed.scheme}://{domain}/robots.txt"
                rp = urllib.robotparser.RobotFileParser()
                rp.set_url(robots_url)
                try:
                    rp.read()
                except:
                    pass  # If robots.txt can't be read, assume allowed
                self.robot_parsers[domain] = rp
        
        try:
            return self.robot_parsers[domain].can_fetch("*", url)
        except:
            return True  # If check fails, assume allowed
    
    def calculate_priority(self, url, depth):
        """Calculate priority score for URL (higher = better)"""
        domain = self.get_domain(url)
        superdomain = self.get_superdomain(domain)
        
        with self.domain_lock:
            # Domain novelty score: 1 / lg(pages_from_domain)
            domain_pages = self.domain_counts.get(domain, 0) + 1
            domain_score = 1.0 / math.log2(domain_pages + 1)
            
            # Superdomain novelty score
            superdomain_domains = self.superdomain_counts.get(superdomain, 0) + 1
            superdomain_score = 1.0 / math.log2(superdomain_domains + 1)
        
        # Combine scores (weights can be adjusted)
        priority = domain_score * 0.7 + superdomain_score * 0.3
        
        # Slightly penalize deeper pages
        priority -= depth * 0.01
        
        return priority, domain_score, superdomain_score
    
    def wait_for_domain_politeness(self, domain):
        """Wait if necessary to respect politeness delay for domain"""
        with self.domain_access_lock:
            if domain in self.domain_last_access:
                elapsed = time.time() - self.domain_last_access[domain]
                if elapsed < self.politeness_delay:
                    time.sleep(self.politeness_delay - elapsed)
            self.domain_last_access[domain] = time.time()
    
    def add_to_queue(self, url, depth):
        """Add URL to queue with priority (thread-safe)"""
        url = self.normalize_url(url)
        
        with self.visited_lock:
            if url in self.visited:
                return
        
        if not self.should_crawl(url):
            return
        
        if not self.check_robots_txt(url):
            return
        
        priority, _, _ = self.calculate_priority(url, depth)
        
        # PriorityQueue uses min-heap, so negate for max priority
        self.queue.put((-priority, url, depth))
    
    def download_page(self, url):
        """Download page and return content, size, and status"""
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; WebCrawler/1.0)'
        })
        
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                # Check MIME type
                content_type = response.headers.get('Content-Type', '').lower()
                if 'text/html' not in content_type:
                    return None, 0, response.status, response.geturl()
                
                content = response.read()
                final_url = response.geturl()  # Handle redirects
                return content, len(content), response.status, final_url
        
        except urllib.error.HTTPError as e:
            return None, 0, e.code, url
        except urllib.error.URLError as e:
            return None, 0, 0, url
        except Exception as e:
            return None, 0, -1, url
    
    def extract_links(self, content, base_url):
        """Extract and resolve links from HTML content"""
        try:
            html = content.decode('utf-8', errors='ignore')
        except:
            return []
        
        parser = LinkExtractor(base_url)
        try:
            parser.feed(html)
        except:
            return []
        
        # Resolve relative URLs
        base = parser.base_href if parser.base_href else base_url
        resolved_links = []
        
        for link in parser.links:
            try:
                absolute_url = urllib.parse.urljoin(base, link)
                resolved_links.append(absolute_url)
            except:
                continue
        
        return resolved_links
    
    def log_visit(self, url, size, status, depth, domain_score, superdomain_score):
        """Log visit information (thread-safe)"""
        timestamp = datetime.now().isoformat()
        log_entry = {
            'timestamp': timestamp,
            'url': url,
            'size_bytes': size,
            'status_code': status,
            'depth': depth,
            'domain_priority': round(domain_score, 4),
            'superdomain_priority': round(superdomain_score, 4)
        }
        
        with self.log_lock:
            self.log_file.write(json.dumps(log_entry) + '\n')
            self.log_file.flush()
    
    def crawl_worker(self, thread_id):
        """Worker thread function"""
        while not self.stop_crawling:
            try:
                # Get URL from queue with timeout
                neg_priority, url, depth = self.queue.get(timeout=2)
            except Empty:
                # Queue is empty, check if we should stop
                continue
            
            # Check if already visited (double-check with lock)
            with self.visited_lock:
                if url in self.visited:
                    self.queue.task_done()
                    continue
                # Mark as being processed
                self.visited[url] = {'processing': True}
            
            # Check if we've reached max pages
            with self.crawl_lock:
                if self.pages_crawled >= self.max_pages:
                    self.stop_crawling = True
                    self.queue.task_done()
                    break
            
            # Respect domain politeness
            domain = self.get_domain(url)
            self.wait_for_domain_politeness(domain)
            
            # Download page
            content, size, status, final_url = self.download_page(url)
            
            # Normalize final URL (after redirects)
            final_url = self.normalize_url(final_url)
            
            # Calculate scores
            _, domain_score, superdomain_score = self.calculate_priority(final_url, depth)
            
            # Log visit
            self.log_visit(final_url, size, status, depth, domain_score, superdomain_score)
            
            # Update visited and counts
            with self.visited_lock:
                self.visited[final_url] = {
                    'depth': depth,
                    'status': status,
                    'size': size
                }
            
            with self.domain_lock:
                domain = self.get_domain(final_url)
                superdomain = self.get_superdomain(domain)
                
                if domain not in self.domain_counts:
                    self.domain_counts[domain] = 0
                self.domain_counts[domain] += 1
                
                if self.domain_counts[domain] == 1:
                    if superdomain not in self.superdomain_counts:
                        self.superdomain_counts[superdomain] = 0
                    self.superdomain_counts[superdomain] += 1
            
            with self.crawl_lock:
                self.pages_crawled += 1
                current_count = self.pages_crawled
            
            print(f"[Thread-{thread_id}] [{current_count}/{self.max_pages}] {url[:80]} (depth={depth}, status={status})")
            
            # Extract and queue links if successful
            if status == 200 and content:
                links = self.extract_links(content, final_url)
                
                for link in links:
                    if not self.stop_crawling:
                        self.add_to_queue(link, depth + 1)
            
            self.queue.task_done()
    
    def get_search_results(self, query, num_results=10):
        """Get seed URLs from a search (simplified)"""
        print(f"Note: For seed URLs, please provide starting URLs manually")
        print(f"In a production system, integrate with Google Custom Search API or similar")
        
        # Return some example seed URLs
        seeds = [
            f"https://www.google.com/search?q={query.replace(' ', '+')}",
            f"https://www.bing.com/search?q={query.replace(' ', '+')}",
            f"https://duckduckgo.com/?q={query.replace(' ', '+')}",
        ]
        return seeds
    
    def crawl(self, query, seed_urls=None):
        """Main crawling function with multithreading"""
        print(f"Starting multithreaded crawl with {self.num_threads} threads")
        print(f"Query: '{query}'")
        
        # Open log file
        log_filename = f"crawl_log_{int(time.time())}.jsonl"
        self.log_file = open(log_filename, 'w')
        
        # Initialize queue with seed URLs
        if seed_urls is None:
            seed_urls = self.get_search_results(query)
        
        print(f"Seed URLs: {len(seed_urls)}")
        for url in seed_urls:
            self.add_to_queue(url, depth=0)
        
        # Start worker threads
        threads = []
        for i in range(self.num_threads):
            t = threading.Thread(target=self.crawl_worker, args=(i,))
            t.daemon = True
            t.start()
            threads.append(t)
        
        # Wait for completion or max pages reached
        try:
            while not self.stop_crawling:
                time.sleep(1)
                with self.crawl_lock:
                    if self.pages_crawled >= self.max_pages:
                        self.stop_crawling = True
                        break
                
                # Check if queue is empty and all threads are waiting
                if self.queue.empty():
                    time.sleep(2)
                    if self.queue.empty():
                        self.stop_crawling = True
                        break
        
        except KeyboardInterrupt:
            print("\nCrawl interrupted by user")
            self.stop_crawling = True
        
        # Wait for all threads to finish
        print("\nWaiting for threads to finish...")
        for t in threads:
            t.join(timeout=5)
        
        self.log_file.close()
        
        print(f"\nCrawl complete!")
        print(f"Pages crawled: {self.pages_crawled}")
        print(f"Unique domains: {len(self.domain_counts)}")
        print(f"Log file: {log_filename}")
        
        return log_filename

# Example usage
if __name__ == "__main__":
    # Create crawler with 8 threads
    
    # Provide your own seed URLs here
    # seed_urls = [
    #     "https://en.wikipedia.org/wiki/Web_crawler",
    #     "https://www.python.org/",
    #     "https://news.ycombinator.com/",
    #     "https://en.wikipedia.org/wiki/World_Wide_Web",
    #     "https://www.reddit.com/r/programming/",
    # ]

    query = input("Enter search query: ")
    
    crawler = WebCrawler(query, max_pages=500, timeout=5, num_threads=36)


    log_file = crawler.crawl(query)
    
    print(f"\nCrawl statistics:")
    print(f"Total pages visited: {len(crawler.visited)}")
    print(f"Domains crawled: {len(crawler.domain_counts)}")
    print(f"Top 10 domains by page count:")
    sorted_domains = sorted(crawler.domain_counts.items(), key=lambda x: x[1], reverse=True)
    for domain, count in sorted_domains[:10]:
        print(f"  {domain}: {count} pages")