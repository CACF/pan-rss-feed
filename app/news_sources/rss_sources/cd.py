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


class CoinDeskRSSPipeline:
    SOURCE = "CoinDesk"
    RSS_FEEDS = [
        "https://www.coindesk.com/arc/outboundfeeds/rss",
    ]

    headers = {
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }

    @staticmethod
    def parse_date(date_str):
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%SZ",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_content(html):
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        return re.sub(r"http\S+|www\.\S+", "", text)

    @staticmethod
    def fetch_full_description(link):
        """
        Fetch full CoinDesk article content.
        Uses stable selectors.
        """
        content = None
        author = "Unknown"

        try:
            with cloudscraper.create_scraper() as scraper:
                res = scraper.get(
                    link,
                    timeout=15,
                    headers=get_random_headers(CoinDeskRSSPipeline.headers),
                )

                if res.status_code != 200:
                    return None, author

                soup = BeautifulSoup(res.content, "lxml")

                author_elems = soup.select(
                    'div.font-sans a:first-of-type'
                )
                if author_elems:
                    author = ", ".join(
                        a.get_text(strip=True) for a in author_elems
                    )
                paragraphs = soup.select("div[data-module-name='article-body'] p")

                text_parts = [
                    re.sub(r"http\S+|www\.\S+", "", p.get_text(strip=True))
                    for p in paragraphs
                    if p.get_text(strip=True)
                ]

                if text_parts:
                    content = " ".join(text_parts)

                res.close()

        except Exception as e:
            logger.warning(f"CoinDesk article fetch failed: {link} | {e}")

        return content, author

    @staticmethod
    def fetch_coindesk_rss_feed(feed_url):
        try:
            logger.info(f"Fetching CoinDesk RSS feed: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url,
                    timeout=15,
                    headers=get_random_headers(CoinDeskRSSPipeline.headers),
                )
                response.raise_for_status()
                payload = response.content
                response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")

            feed_date_elem = soup.find("lastBuildDate")
            feed_date = (
                CoinDeskRSSPipeline.parse_date(feed_date_elem.text)
                if feed_date_elem
                else datetime.now(timezone.utc)
            )

            base_articles = []
            for item in items:
                title = item.title.get_text(strip=True)
                link = item.link.get_text(strip=True)

                pub_date = (
                    CoinDeskRSSPipeline.parse_date(item.pubDate.text)
                    if item.pubDate
                    else datetime.now(timezone.utc)
                )

                desc = (
                    CoinDeskRSSPipeline.clean_content(item.description.text)
                    if item.description
                    else ""
                )
                creators = item.find_all("dc:creator")
                rss_authors = ", ".join(c.get_text(strip=True) for c in creators)

                base_articles.append(
                    {
                        "title": title,
                        "link": link,
                        "description": desc,
                        "pub_date": pub_date,
                        "feed_date": feed_date,
                        "rss_authors": rss_authors,
                    }
                )

            articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_map = {
                    executor.submit(
                        CoinDeskRSSPipeline.fetch_full_description, a["link"]
                    ): a
                    for a in base_articles
                }

                for future in concurrent.futures.as_completed(future_map):
                    base = future_map[future]
                    try:
                        full_content, html_author = future.result()

                        content = full_content or base["description"]
                        author = html_author or base["rss_authors"] or "Unknown"

                        if not content:
                            continue

                        article = {
                            "_id": base["link"],
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": base["pub_date"],
                            "feedBuildDate": base["feed_date"],
                            "title": base["title"],
                            "authors": author,
                            "language": "en-us",
                            "source": CoinDeskRSSPipeline.SOURCE,
                            "content": content,
                            "genre": "Crypto",
                            "media_origin": "foreign",
                            "tags": [],
                        }

                        articles.append(article)

                    except Exception as e:
                        logger.warning(f"CoinDesk article processing error: {e}")

            logger.info(f"Parsed {len(articles)} valid CoinDesk articles")
            return articles

        except Exception as e:
            logger.error(f"CoinDesk RSS fetch failed: {e}")
            return []

    @staticmethod
    def process_input(input_data=None):
        all_articles = []
        for feed in CoinDeskRSSPipeline.RSS_FEEDS:
            all_articles.extend(
                CoinDeskRSSPipeline.fetch_coindesk_rss_feed(feed)
            )
        logger.info(f"CoinDesk RSS pipeline processed {len(all_articles)} articles")
        return all_articles

    @staticmethod
    def run_pipeline(input_data=None):
        t0 = time.perf_counter()
        articles = CoinDeskRSSPipeline.process_input(input_data)

        if not articles:
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "elapsed_time": round(time.perf_counter() - t0, 2),
            }

        result = MongoDBClient.insert_articles_to_mongo(
            articles,
            user_email=input_data.get("email") if input_data else None,
        )
        result["elapsed_time"] = round(time.perf_counter() - t0, 2)

        logger.info(f"CoinDesk pipeline finished in {result['elapsed_time']}s")
        return result
