from config import FASHION_TABLE
# wwd_pipeline.py
# Skeleton WWD RSS Pipeline based on FashionTimesRSSPipeline

import re, uuid, json, logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class WWDRSSPipeline:
    SOURCE = "WWD"
    RSS_FEEDS = ["https://wwd.com/fashion-news/feed/"]

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return datetime.now(timezone.utc)
        for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z"):
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.astimezone(timezone.utc)
            except:
                pass
        return datetime.now(timezone.utc)

    @staticmethod
    def clean_text(html):
        soup = BeautifulSoup(html or "", "html.parser")
        for t in soup(
            ["script", "style", "iframe", "noscript", "img", "figure", "svg"]
        ):
            t.decompose()
        txt = soup.get_text(" ", strip=True)
        txt = re.sub(r"http\S+|www\.\S+", "", txt)
        return " ".join(txt.split())

    @staticmethod
    def fetch_article_content(url):
        try:
            with cloudscraper.create_scraper() as s:
                r = s.get(url, headers=get_random_headers(), timeout=30)
                html = r.text

            soup = BeautifulSoup(html, "html.parser")

            img = None
            image = soup.select_one("meta[property='og:image']")
            if image and image.get("content"):
                img = image["content"]

            # Primary extraction
            content_div = soup.select_one("#article-content .pmc-not-a-paywall")

            if content_div:
                paragraphs = content_div.select("p.paragraph")
            else:
                # Fallback
                paragraphs = soup.select("p.paragraph.larva")

            text = "\n\n".join(
                p.get_text(" ", strip=True)
                for p in paragraphs
                if p.get_text(strip=True)
            )

            return WWDRSSPipeline.clean_text(text), img

        except Exception:
            return "", None

    @staticmethod
    def fetch_rss_feed(feed_url):
        arts = []
        with cloudscraper.create_scraper() as s:
            r = s.get(feed_url, headers=get_random_headers(), timeout=30)
            soup = BeautifulSoup(r.content, "xml")
        build = datetime.now(timezone.utc)
        for item in soup.find_all("item"):
            try:
                title = item.title.get_text(strip=True)
                link = item.link.get_text(strip=True)
                pub = WWDRSSPipeline.parse_date(
                    item.pubDate.get_text() if item.pubDate else ""
                )
                cats = [c.get_text(strip=True) for c in item.find_all("category")]
                author = item.find("dc:creator")
                author = author.get_text(strip=True) if author else "WWD"

                content = None
                desc, img = WWDRSSPipeline.fetch_article_content(link)
                if desc:
                    content = WWDRSSPipeline.clean_text(desc)

                if len(content) < 200:
                    continue

                image = img
                arts.append(
                    {
                        "id": link,
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": pub,
                        "feedBuildDate": build,
                        "title": title,
                        "authors": author,
                        "language": "en-US",
                        "image": image,
                        "source": WWDRSSPipeline.SOURCE,
                        "content": content,
                        "genre": "Fashion",
                        "media_origin": "international",
                        "tags": cats,
                    }
                )
            except Exception as e:
                logger.warning(e)
        return arts

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        target_table = FASHION_TABLE
        all_articles = []
        for f in WWDRSSPipeline.RSS_FEEDS:
            all_articles.extend(WWDRSSPipeline.fetch_rss_feed(f))
        if not all_articles:
            return {"inserted_count": 0, "total_articles": 0}
        return SupabaseClient.insert_articles_current_year(all_articles, table_name=target_table)
