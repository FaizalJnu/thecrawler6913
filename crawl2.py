import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import heapq
import time
from ddgs import DDGS


class AsyncPriorityCrawler:
    def __init__(self, start_urls, max_pages=1000, concurrency=50, timeout=8):
        self.visited = set()
        self.max_pages = max_pages
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(concurrency)

        # Priority queue: (priority, url)
        self.queue = []
        for url in start_urls:
            heapq.heappush(self.queue, (0, url))

    def is_trap(self, url):
        """Heuristics for URL traps"""
        if url.count("?") > 3 or len(url) > 200:
            return True
        bad_patterns = ["calendar", "date=", "sessionid", "replytocom"]
        return any(p in url.lower() for p in bad_patterns)

    def get_priority(self, url):
        """Favor unexplored domains"""
        domain = urlparse(url).netloc
        domain_count = sum(1 for u in self.visited if urlparse(u).netloc == domain)
        return domain_count * 10

    async def fetch(self, session, url):
        """Async HTTP GET"""
        try:
            async with self.semaphore:
                async with session.get(url, timeout=self.timeout) as resp:
                    if resp.content_type != "text/html":
                        return None
                    return await resp.text()
        except Exception:
            return None

    async def process_page(self, url, html):
        """Parse page and extract links"""
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            new_url = urljoin(url, a["href"])
            if not self.is_trap(new_url):
                links.append(new_url)
        return links

    async def crawl(self):
        pages_crawled = 0

        async with aiohttp.ClientSession() as session:
            while self.queue and pages_crawled < self.max_pages:
                priority, url = heapq.heappop(self.queue)

                if url in self.visited or self.is_trap(url):
                    continue

                html = await self.fetch(session, url)
                if not html:
                    continue

                self.visited.add(url)
                pages_crawled += 1
                print(f"[{pages_crawled}] {url} (priority {priority})")

                # Process page links
                links = await self.process_page(url, html)
                for link in links:
                    if link not in self.visited:
                        pr = self.get_priority(link)
                        heapq.heappush(self.queue, (pr, link))

        print(f"✅ Finished crawling {pages_crawled} pages.")

def get_seed_urls(query, max_results):
    results = DDGS().text("query", max_results)
    urls = []
    for r in results:
        href = r.get("href")
        if href:
            urls.append(href)
        if len(urls) >= max_results:
            break
    return urls

if __name__ == "__main__":
    query = input("Enter your search query: ").strip()
    seed_urls = get_seed_urls(query, max_results=10)
    print("Seed URLs:", seed_urls)

    if not seed_urls:
        print("No seed URLs found for query. Exiting.")
    else:
        crawler = AsyncPriorityCrawler(start_urls=seed_urls,
                                       max_pages=200,
                                       concurrency=100)
        asyncio.run(crawler.crawl())
