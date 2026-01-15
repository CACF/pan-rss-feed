import uuid
import time
import logging
import re
import concurrent.futures
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper
from app.utilities import MongoDBClient, get_random_headers

logger = logging.getLogger(__name__)

class BBCRSSPipeline:
    SOURCE = "BBC Business"
    RSS_FEEDS = [
        "https://feeds.bbci.co.uk/news/business/rss.xml",
    ]
    headers = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }

    @staticmethod
    def parse_date(date_str):
        formats = [
            "%a, %d %b %Y %H:%M:%S %Z",
            "%a, %d %b %Y %H:%M:%S GMT",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(content_html):
        if not content_html:
            return ""
        try:
            soup = BeautifulSoup(content_html, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            text = soup.get_text()
            text = re.sub(r"http\S+|www\.\S+", "", text)
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            clean_text = " ".join(chunk for chunk in chunks if chunk)
            return clean_text
        except Exception as e:
            logger.warning(f"Failed to clean content: {e}")
            return content_html

    @staticmethod
    def fetch_full_description(link):
        """Fetch full article content via cloudscraper, skip if <200 chars."""
        full_description = None
        author = "Unknown"
        try:
            with cloudscraper.create_scraper() as scraper:
                res = scraper.get(link, timeout=15, headers=get_random_headers(BBCRSSPipeline.headers))
                try:
                    if res.status_code != 200:
                        return None, author

                    soup = BeautifulSoup(res.content, "lxml")
                    author_elem = soup.find("span", class_="ssrcss-1rv0g9w-Contributor")
                    if author_elem:
                        author = author_elem.get_text(strip=True)

                    paragraphs = soup.select("div.sc-3b6b161a-0 p")
                    text_parts = [re.sub(r"http\S+|www\.\S+", "", p.get_text(strip=True)) for p in paragraphs]
                    text_parts = [t for t in text_parts if t]

                    if text_parts:
                        full_text = " ".join(text_parts)
                        if len(full_text) < 200:
                            return None, author
                        # Limit to 200 words max
                        words = full_text.split()
                        if len(words) > 200:
                            full_text = " ".join(words[:200])
                        full_description = full_text
                finally:
                    res.close()
        except Exception as e:
            logger.warning(f"Failed to fetch full description from {link}: {e}")
        return full_description, author

    @staticmethod
    def fetch_bbc_rss_feed(feed_url):
        try:
            logger.info(f"Fetching BBC RSS feed: {feed_url}")
            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(feed_url, timeout=15, headers=get_random_headers(BBCRSSPipeline.headers))
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")
            feed_build_date_str = soup.find("lastBuildDate")
            feed_build_date = BBCRSSPipeline.parse_date(feed_build_date_str.text) if feed_build_date_str else datetime.now(timezone.utc)

            base_articles = []
            for item in items:
                title_elem = item.find("title")
                link_elem = item.find("link")
                desc_elem = item.find("description")
                pub_date_elem = item.find("pubDate")

                if not title_elem or not link_elem:
                    continue

                title = title_elem.get_text(strip=True)
                link = link_elem.get_text(strip=True)
                desc = BBCRSSPipeline.clean_content(desc_elem.get_text()) if desc_elem else ""
                pub_date = BBCRSSPipeline.parse_date(pub_date_elem.get_text()) if pub_date_elem else datetime.now(timezone.utc)

                # Skip short RSS description
                if len(desc) < 200:
                    desc = None

                base_articles.append({
                    "title": title,
                    "link": link,
                    "description": desc,
                    "pub_date": pub_date,
                    "feed_date": feed_build_date
                })

            articles = []
            # Fetch full descriptions only if RSS description < 200
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_article = {
                    executor.submit(BBCRSSPipeline.fetch_full_description, a["link"]): a for a in base_articles
                }
                for future in concurrent.futures.as_completed(future_to_article):
                    base = future_to_article[future]
                    try:
                        full_description, author = future.result()
                        description = base["description"] or full_description
                        if not description or len(description) < 200:
                            continue

                        article = {
                            "_id": base["link"],
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": base["pub_date"],
                            "feedBuildDate": base["feed_date"],
                            "title": base["title"],
                            "authors": author,
                            "language": "en-us",
                            "source": BBCRSSPipeline.SOURCE,
                            "content": description,
                            "genre": "Business",
                            "media_origin": "foreign",
                            "tags": [],
                        }
                        articles.append(article)
                    except Exception as e:
                        logger.warning(f"Error processing BBC article: {e}")

            logger.info(f"Parsed {len(articles)} valid BBC business articles")
            return articles
        except Exception as e:
            logger.error(f"Failed to fetch BBC RSS feed {feed_url}: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        try:
            logger.info("Starting BBC RSS pipeline processing")
            all_articles = []
            for feed_url in BBCRSSPipeline.RSS_FEEDS:
                articles = BBCRSSPipeline.fetch_bbc_rss_feed(feed_url)
                all_articles.extend(articles)
            logger.info(f"BBC RSS pipeline processed {len(all_articles)} total articles")
            return all_articles
        except Exception as e:
            logger.error(f"BBC RSS pipeline processing failed: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            logger.info("Starting BBC RSS pipeline")
            t0 = time.perf_counter()
            articles = BBCRSSPipeline.process_input(input_data)
            if not articles:
                elapsed = time.perf_counter() - t0
                return {"inserted_count": 0, "total_articles": 0, "elapsed_time": round(elapsed, 2)}

            result = MongoDBClient.insert_articles_to_mongo(
                articles, user_email=input_data.get("email") if input_data else None
            )
            result["elapsed_time"] = round(time.perf_counter() - t0, 2)
            logger.info(f"BBC pipeline finished in {result['elapsed_time']:.2f}s")
            return result
        except Exception as e:
            elapsed = time.perf_counter()
            logger.error(f"BBC RSS pipeline failed: {e}")
            return {"inserted_count": 0, "total_articles": 0, "error": str(e), "elapsed_time": round(elapsed, 2)}
