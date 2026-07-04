import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from googlenewsdecoder import new_decoderv1

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class HoustonPulseRSSPipeline:
    """
    Houston Pulse — Targeted News Pipeline for Pakistani-American Community in Houston, Texas
    """

    SOURCE = "Google News"

    GOOGLE_NEWS_FEEDS = [
        "https://news.google.com/rss/search?q=Pakistani+community+Houston+when:7d&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=Pakistan+Houston+when:7d&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=Houston+mosque+when:7d&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=Pakistani+American+Houston&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=Pakistani+American+Houston+when:30d&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=Pakistani+American+Houston+when:7d&hl=en-US&gl=US&ceid=US:en",
        "https://news.google.com/rss/search?q=Houston+Pakistani+when:7d&hl=en-US&gl=US&ceid=US:en",
    ]

    HOUSTON_LOCAL_FEEDS = [
        "https://chron.com/rss/feed/News-270.php",  # Houston Chronicle
        "https://www.houstonpublicmedia.org/feed",  # Houston Public Media (NPR)
        "https://abc13.com/feed",  # ABC13 / KTRK
    ]

    PAKISTAN_KEYWORDS = [
        "pakistan",
        "pakistani",
        "karachi",
        "lahore",
        "islamabad",
        "sindh",
        "punjab",
        "peshawar",
        "urdu",
        "south asian",
        "pagh",
        "hksca",
        "pakistani-american",
        "pakistani american",
    ]

    DATE_META_CANDIDATES = [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"name": "article:published_time"}),
        ("meta", {"property": "og:published_time"}),
        ("meta", {"name": "publish-date"}),
        ("meta", {"name": "publishdate"}),
        ("meta", {"name": "date"}),
        ("meta", {"name": "sailthru.date"}),
        ("meta", {"itemprop": "datePublished"}),
    ]

    @staticmethod
    def parse_date(date_str):
        if not date_str:
            return None

        date_str = date_str.strip()
        date_str = date_str.replace("UT", "UTC")

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return (
                    dt.astimezone(timezone.utc)
                    if dt.tzinfo
                    else dt.replace(tzinfo=timezone.utc)
                )
            except Exception:
                continue

        logger.warning(f"Unrecognized date format: {date_str}")
        return None

    @staticmethod
    def extract_published_date(soup):
        for tag_name, attrs in HoustonPulseRSSPipeline.DATE_META_CANDIDATES:
            tag = soup.find(tag_name, attrs=attrs)
            if tag and tag.get("content"):
                parsed = HoustonPulseRSSPipeline.parse_date(tag["content"])
                if parsed:
                    return parsed

        time_tag = soup.find("time")
        if time_tag and time_tag.get("datetime"):
            parsed = HoustonPulseRSSPipeline.parse_date(time_tag["datetime"])
            if parsed:
                return parsed

        return None

    @staticmethod
    def clean_text(html):
        if not html:
            return ""
        try:
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["script", "style", "iframe", "noscript", "img", "figure"]):
                tag.decompose()
            for a_tag in soup.find_all("a"):
                a_tag.unwrap()
            text = soup.get_text(separator=" ")
            text = re.sub(r"http\S+|www\.\S+", "", text)
            return " ".join(text.split())
        except Exception as e:
            logger.warning(f"Content cleaning failed: {e}")
            return html or ""

    @staticmethod
    def extract_image(soup):
        for attr, key in [
            ("property", "og:image"),
            ("name", "og:image"),
            ("property", "og:image:secure_url"),
            ("name", "twitter:image"),
            ("property", "twitter:image"),
            ("name", "twitter:image:src"),
        ]:
            tag = soup.find("meta", attrs={attr: key})
            if tag and tag.get("content", "").startswith("http"):
                return tag["content"]

        for sel in [
            "article",
            "[class*='article-body']",
            "[class*='story-body']",
            "main",
        ]:
            container = soup.select_one(sel)
            if container:
                img = container.find("img")
                if img:
                    src = img.get("src") or img.get("data-src")
                    if src and src.startswith("http"):
                        return src

        return None

    @staticmethod
    def extract_tags_from_article(soup):
        """
        RSS <category> tags are almost never present on Google News search
        feeds. The article page itself usually carries this info instead:
          - meta name="keywords" content="a, b, c"
          - meta property="article:tag" (can repeat — one tag per meta tag)
          - meta name="news_keywords" (some publishers use this instead)
        Returns a deduped list of tag strings (may be empty).
        """
        tags = []

        keywords_tag = soup.find("meta", attrs={"name": "keywords"}) or soup.find(
            "meta", attrs={"name": "news_keywords"}
        )
        if keywords_tag and keywords_tag.get("content"):
            tags.extend(
                [t.strip() for t in keywords_tag["content"].split(",") if t.strip()]
            )

        for tag in soup.find_all("meta", attrs={"property": "article:tag"}):
            if tag.get("content"):
                tags.append(tag["content"].strip())

        section_tag = soup.find("meta", attrs={"property": "article:section"})
        if section_tag and section_tag.get("content"):
            tags.append(section_tag["content"].strip())

        # Dedupe while preserving order, case-insensitively.
        seen = set()
        deduped = []
        for t in tags:
            key = t.lower()
            if key not in seen:
                seen.add(key)
                deduped.append(t)

        return deduped

    @staticmethod
    def extract_author_from_title(title):
        """
        Google News titles are formatted "Headline - Publisher Name".
        Used as a last-resort fallback if neither the RSS <source> tag nor
        dc:creator/author gave us anything.
        """
        if " - " in title:
            return title.rsplit(" - ", 1)[-1].strip()
        return None

    @staticmethod
    def resolve_author(item, title, fallback_source_name):
        """
        Priority:
          1. RSS <source> tag — this is the actual publisher name Google
             News attaches to each item (e.g. "ABC13 Houston"). Not the
             same as dc:creator/author, which Google News feeds don't set.
          2. dc:creator / author tag — used by non-Google local feeds.
          3. Parsed from the "Headline - Publisher" title suffix.
          4. fallback_source_name (SOURCE) as a last resort.
        """
        source_elem = item.find("source")
        if source_elem and source_elem.get_text(strip=True):
            return source_elem.get_text(strip=True)

        author_elem = item.find("dc:creator") or item.find("author")
        if author_elem and author_elem.get_text(strip=True):
            return author_elem.get_text(strip=True)

        from_title = HoustonPulseRSSPipeline.extract_author_from_title(title)
        if from_title:
            return from_title

        return fallback_source_name

    @staticmethod
    def is_pakistan_related(text):
        lower = text.lower()
        return any(kw in lower for kw in HoustonPulseRSSPipeline.PAKISTAN_KEYWORDS)

    @staticmethod
    def resolve_google_news_link(google_url):
        if not google_url:
            return None
        if "news.google.com" not in google_url:
            return google_url
        try:
            result = new_decoderv1(google_url, interval=1)
            if result.get("status"):
                return result.get("decoded_url")
            logger.warning(f"Google News decode failed: {result.get('message')}")
            return None
        except Exception as e:
            logger.warning(f"Google News decode error for {google_url}: {e}")
            return None

    @staticmethod
    def full_description(link):
        result = {"content": "", "image": None, "published": None, "tags": []}

        if not link:
            return result

        try:
            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(link, timeout=30, headers=get_random_headers())
                response.raise_for_status()
                html = response.text

            soup = BeautifulSoup(html, "lxml")

            result["image"] = HoustonPulseRSSPipeline.extract_image(soup)
            result["published"] = HoustonPulseRSSPipeline.extract_published_date(soup)
            result["tags"] = HoustonPulseRSSPipeline.extract_tags_from_article(soup)

            selectors = [
                "article",
                "[class*='article-body']",
                "[class*='story-body']",
                "[class*='story-content']",
                "[class*='entry-content']",
                "[class*='post-content']",
                "[class*='article-content']",
                "[class*='detail-content']",
                "[class*='content-body']",
                "[class*='news-body']",
                "main",
            ]

            paragraphs = []
            for sel in selectors:
                container = soup.select_one(sel)
                if container:
                    for p in container.find_all("p"):
                        text = p.get_text(strip=True)
                        if len(text) >= 30:
                            paragraphs.append(HoustonPulseRSSPipeline.clean_text(text))
                    if paragraphs:
                        break

            if not paragraphs:
                for p in soup.find_all("p"):
                    text = p.get_text(strip=True)
                    if len(text) >= 30:
                        paragraphs.append(HoustonPulseRSSPipeline.clean_text(text))

            result["content"] = " ".join(paragraphs)

        except Exception as e:
            logger.warning(f"Failed to fetch full article {link}: {e}")

        return result

    @staticmethod
    def fetch_rss_feed(feed_url, is_google_news, apply_pakistan_filter):
        try:
            logger.info(f"Fetching RSS: {feed_url}")

            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(
                    feed_url, timeout=30, headers=get_random_headers()
                )
                try:
                    response.raise_for_status()
                    payload = response.content
                finally:
                    response.close()

            soup = BeautifulSoup(payload, "lxml-xml")
            items = soup.find_all("item")[:30]
            feed_build_date = datetime.now(timezone.utc)
            articles = []

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pubdate_elem = item.find("pubDate")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    raw_link = link_elem.get_text(strip=True)

                    if is_google_news:
                        link = HoustonPulseRSSPipeline.resolve_google_news_link(
                            raw_link
                        )
                        if not link:
                            logger.info(
                                f"Could not resolve Google News link, skipping: '{title}'"
                            )
                            continue
                    else:
                        link = raw_link

                    rss_pub_date = (
                        HoustonPulseRSSPipeline.parse_date(pubdate_elem.get_text())
                        if pubdate_elem
                        else None
                    ) or datetime.now(timezone.utc)

                    author = HoustonPulseRSSPipeline.resolve_author(
                        item, title, HoustonPulseRSSPipeline.SOURCE
                    )

                    # RSS-level categories (rare on Google News, more common
                    # on local feeds) — combined with article-page tags below.
                    rss_categories = [
                        c.get_text(strip=True)
                        for c in item.find_all("category")
                        if c.get_text(strip=True)
                    ]

                    content_elem = item.find("content:encoded") or item.find(
                        "description"
                    )
                    content_raw = content_elem.get_text() if content_elem else ""
                    content = HoustonPulseRSSPipeline.clean_text(content_raw)

                    if apply_pakistan_filter:
                        if not HoustonPulseRSSPipeline.is_pakistan_related(
                            title + " " + content
                        ):
                            logger.debug(
                                f"Not Pakistan-related (pre-filter), skipping: '{title}'"
                            )
                            continue

                    logger.info(f"Full fetch for '{title}' — {link}")
                    full = HoustonPulseRSSPipeline.full_description(link)

                    if full["content"] and len(full["content"]) > len(content):
                        content = full["content"]

                    image_url = full["image"]
                    pub_date = full["published"] or rss_pub_date

                    # Merge RSS categories with article-page tags, dedupe.
                    seen = set()
                    categories = []
                    for tag in rss_categories + full["tags"]:
                        key = tag.lower()
                        if key not in seen:
                            seen.add(key)
                            categories.append(tag)

                    if len(content) < 200:
                        logger.info(f"Skipped (too short after full fetch): '{title}'")
                        continue

                    if apply_pakistan_filter:
                        if not HoustonPulseRSSPipeline.is_pakistan_related(
                            title + " " + content
                        ):
                            logger.debug(
                                f"Not Pakistan-related (post-fetch), skipping: '{title}'"
                            )
                            continue

                    articles.append(
                        {
                            "id": link,
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": pub_date,
                            "feedBuildDate": feed_build_date,
                            "title": title,
                            "authors": author,
                            "language": "en-US",
                            "image": image_url,
                            "source": HoustonPulseRSSPipeline.SOURCE,
                            "content": content,
                            "genre": "News",
                            "media_origin": "international",
                            "tags": categories,
                        }
                    )

                    logger.info(
                        f"Added: '{title}' | {len(content)} chars | "
                        f"author={author} | tags={len(categories)} | "
                        f"image={'yes' if image_url else 'no'} | "
                        f"date={'article' if full['published'] else 'rss-fallback'}"
                    )

                except Exception as e:
                    logger.warning(f"Failed parsing item: {e}")
                    continue

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"RSS fetch failed [{feed_url}]: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None):
        try:
            all_articles = []

            logger.info("── Tier 1: Google News (Pakistani-American + Houston) ──")
            for feed_url in HoustonPulseRSSPipeline.GOOGLE_NEWS_FEEDS:
                all_articles.extend(
                    HoustonPulseRSSPipeline.fetch_rss_feed(
                        feed_url, is_google_news=True, apply_pakistan_filter=False
                    )
                )

            logger.info("── Tier 2: Houston local feeds (Pakistan keyword filter) ──")
            for feed_url in HoustonPulseRSSPipeline.HOUSTON_LOCAL_FEEDS:
                all_articles.extend(
                    HoustonPulseRSSPipeline.fetch_rss_feed(
                        feed_url, is_google_news=False, apply_pakistan_filter=True
                    )
                )

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            all_articles = list({a["id"]: a for a in all_articles}.values())

            logger.info(f"After dedupe: {len(all_articles)} total articles")

            return SupabaseClient.insert_articles_current_year(all_articles)

        except Exception as e:
            logger.error(f"Houston Pulse pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }


# import re
# import uuid
# import logging
# from datetime import datetime, timezone
# from bs4 import BeautifulSoup
# import cloudscraper

# from googlenewsdecoder import new_decoderv1

# from app.utilities import get_random_headers
# from app.utils.supabase_client import SupabaseClient

# logger = logging.getLogger(__name__)


# class HoustonPulseRSSPipeline:
#     """
#     Houston Pulse — Targeted News Pipeline for Pakistani-American Community in Houston, Texas
#     """
#     SOURCE = "Google News"

#     GOOGLE_NEWS_FEEDS = [
#         "https://news.google.com/rss/search?q=Pakistani+community+Houston+when:7d&hl=en-US&gl=US&ceid=US:en",
#         "https://news.google.com/rss/search?q=Pakistan+Houston+when:7d&hl=en-US&gl=US&ceid=US:en",
#         "https://news.google.com/rss/search?q=Houston+mosque+when:7d&hl=en-US&gl=US&ceid=US:en",
#         "https://news.google.com/rss/search?q=Pakistani+American+Houston&hl=en-US&gl=US&ceid=US:en",
#         "https://news.google.com/rss/search?q=Pakistani+American+Houston+when:30d&hl=en-US&gl=US&ceid=US:en",
#         "https://news.google.com/rss/search?q=Pakistani+American+Houston+when:7d&hl=en-US&gl=US&ceid=US:en",
#         "https://news.google.com/rss/search?q=Houston+Pakistani+when:7d&hl=en-US&gl=US&ceid=US:en",
#     ]


#     HOUSTON_LOCAL_FEEDS = [
#         "https://chron.com/rss/feed/News-270.php",  # Houston Chronicle
#         "https://www.houstonpublicmedia.org/feed",  # Houston Public Media (NPR)
#         "https://abc13.com/feed",  # ABC13 / KTRK
#     ]

#     PAKISTAN_KEYWORDS = [
#         "pakistan",
#         "pakistani",
#         "karachi",
#         "lahore",
#         "islamabad",
#         "sindh",
#         "punjab",
#         "peshawar",
#         "urdu",
#         "south asian",
#         "pagh",  # Pakistani-American Association of Greater Houston
#         "hksca",  # Houston-Karachi Sister City Association
#         "pakistani-american",
#         "pakistani american",
#     ]

#     # Common date meta tags/attributes, in priority order.
#     DATE_META_CANDIDATES = [
#         ("meta", {"property": "article:published_time"}),
#         ("meta", {"name": "article:published_time"}),
#         ("meta", {"property": "og:published_time"}),
#         ("meta", {"name": "publish-date"}),
#         ("meta", {"name": "publishdate"}),
#         ("meta", {"name": "date"}),
#         ("meta", {"name": "sailthru.date"}),
#         ("meta", {"itemprop": "datePublished"}),
#     ]

#     @staticmethod
#     def parse_date(date_str):
#         if not date_str:
#             return None

#         date_str = date_str.strip()
#         date_str = date_str.replace("UT", "UTC")

#         formats = [
#             "%a, %d %b %Y %H:%M:%S %z",
#             "%a, %d %b %y %H:%M:%S %z",
#             "%a, %d %b %Y %H:%M:%S %Z",
#             "%Y-%m-%dT%H:%M:%SZ",
#             "%Y-%m-%dT%H:%M:%S%z",
#             "%Y-%m-%dT%H:%M:%S.%f%z",
#             "%Y-%m-%dT%H:%M:%S.%fZ",
#             "%Y-%m-%d %H:%M:%S",
#             "%Y-%m-%d",
#         ]

#         for fmt in formats:
#             try:
#                 dt = datetime.strptime(date_str, fmt)
#                 return (
#                     dt.astimezone(timezone.utc)
#                     if dt.tzinfo
#                     else dt.replace(tzinfo=timezone.utc)
#                 )
#             except Exception:
#                 continue

#         logger.warning(f"Unrecognized date format: {date_str}")
#         return None

#     @staticmethod
#     def extract_published_date(soup):
#         for tag_name, attrs in HoustonPulseRSSPipeline.DATE_META_CANDIDATES:
#             tag = soup.find(tag_name, attrs=attrs)
#             if tag and tag.get("content"):
#                 parsed = HoustonPulseRSSPipeline.parse_date(tag["content"])
#                 if parsed:
#                     return parsed

#         time_tag = soup.find("time")
#         if time_tag and time_tag.get("datetime"):
#             parsed = HoustonPulseRSSPipeline.parse_date(time_tag["datetime"])
#             if parsed:
#                 return parsed

#         return None

#     @staticmethod
#     def clean_text(html):
#         if not html:
#             return ""
#         try:
#             soup = BeautifulSoup(html, "html.parser")
#             for tag in soup(["script", "style", "iframe", "noscript", "img", "figure"]):
#                 tag.decompose()
#             for a_tag in soup.find_all("a"):
#                 a_tag.unwrap()
#             text = soup.get_text(separator=" ")
#             text = re.sub(r"http\S+|www\.\S+", "", text)
#             return " ".join(text.split())
#         except Exception as e:
#             logger.warning(f"Content cleaning failed: {e}")
#             return html or ""

#     @staticmethod
#     def extract_image(soup):
#         for attr, key in [
#             ("property", "og:image"),
#             ("name", "og:image"),
#             ("property", "og:image:secure_url"),
#             ("name", "twitter:image"),
#             ("property", "twitter:image"),
#             ("name", "twitter:image:src"),
#         ]:
#             tag = soup.find("meta", attrs={attr: key})
#             if tag and tag.get("content", "").startswith("http"):
#                 return tag["content"]

#         for sel in [
#             "article",
#             "[class*='article-body']",
#             "[class*='story-body']",
#             "main",
#         ]:
#             container = soup.select_one(sel)
#             if container:
#                 img = container.find("img")
#                 if img:
#                     src = img.get("src") or img.get("data-src")
#                     if src and src.startswith("http"):
#                         return src

#         return None

#     @staticmethod
#     def is_pakistan_related(text):
#         lower = text.lower()
#         return any(kw in lower for kw in HoustonPulseRSSPipeline.PAKISTAN_KEYWORDS)

#     @staticmethod
#     def resolve_google_news_link(google_url):
#         if not google_url:
#             return None
#         if "news.google.com" not in google_url:
#             return google_url
#         try:
#             result = new_decoderv1(google_url, interval=1)
#             if result.get("status"):
#                 return result.get("decoded_url")
#             logger.warning(f"Google News decode failed: {result.get('message')}")
#             return None
#         except Exception as e:
#             logger.warning(f"Google News decode error for {google_url}: {e}")
#             return None

#     @staticmethod
#     def full_description(link):
#         result = {"content": "", "image": None, "published": None}

#         if not link:
#             return result

#         try:
#             with cloudscraper.create_scraper() as scraper:
#                 response = scraper.get(link, timeout=30, headers=get_random_headers())
#                 response.raise_for_status()
#                 html = response.text

#             soup = BeautifulSoup(html, "lxml")

#             result["image"] = HoustonPulseRSSPipeline.extract_image(soup)
#             result["published"] = HoustonPulseRSSPipeline.extract_published_date(soup)

#             selectors = [
#                 "article",
#                 "[class*='article-body']",
#                 "[class*='story-body']",
#                 "[class*='story-content']",
#                 "[class*='entry-content']",
#                 "[class*='post-content']",
#                 "[class*='article-content']",
#                 "[class*='detail-content']",
#                 "[class*='content-body']",
#                 "[class*='news-body']",
#                 "main",
#             ]

#             paragraphs = []
#             for sel in selectors:
#                 container = soup.select_one(sel)
#                 if container:
#                     for p in container.find_all("p"):
#                         text = p.get_text(strip=True)
#                         if len(text) >= 30:
#                             paragraphs.append(HoustonPulseRSSPipeline.clean_text(text))
#                     if paragraphs:
#                         break

#             if not paragraphs:
#                 for p in soup.find_all("p"):
#                     text = p.get_text(strip=True)
#                     if len(text) >= 30:
#                         paragraphs.append(HoustonPulseRSSPipeline.clean_text(text))

#             result["content"] = " ".join(paragraphs)

#         except Exception as e:
#             logger.warning(f"Failed to fetch full article {link}: {e}")

#         return result

#     @staticmethod
#     def fetch_rss_feed(feed_url, is_google_news, apply_pakistan_filter):
#         try:
#             logger.info(f"Fetching RSS: {feed_url}")

#             with cloudscraper.create_scraper() as scraper:
#                 response = scraper.get(
#                     feed_url, timeout=30, headers=get_random_headers()
#                 )
#                 try:
#                     response.raise_for_status()
#                     payload = response.content
#                 finally:
#                     response.close()

#             soup = BeautifulSoup(payload, "lxml-xml")
#             items = soup.find_all("item")[:8]
#             feed_build_date = datetime.now(timezone.utc)
#             articles = []

#             for item in items:
#                 try:
#                     title_elem = item.find("title")
#                     link_elem = item.find("link")
#                     pubdate_elem = item.find("pubDate")

#                     if not title_elem or not link_elem:
#                         continue

#                     title = title_elem.get_text(strip=True)
#                     raw_link = link_elem.get_text(strip=True)

#                     if is_google_news:
#                         link = HoustonPulseRSSPipeline.resolve_google_news_link(
#                             raw_link
#                         )
#                         if not link:
#                             logger.info(
#                                 f"Could not resolve Google News link, skipping: '{title}'"
#                             )
#                             continue
#                     else:
#                         link = raw_link

#                     rss_pub_date = (
#                         HoustonPulseRSSPipeline.parse_date(pubdate_elem.get_text())
#                         if pubdate_elem
#                         else None
#                     ) or datetime.now(timezone.utc)

#                     author_elem = item.find("dc:creator") or item.find("author")
#                     author = (
#                         author_elem.get_text(strip=True)
#                         if author_elem
#                         else "Houston Pulse"
#                     )

#                     categories = [
#                         c.get_text(strip=True)
#                         for c in item.find_all("category")
#                         if c.get_text(strip=True)
#                     ]

#                     content_elem = item.find("content:encoded") or item.find(
#                         "description"
#                     )
#                     content_raw = content_elem.get_text() if content_elem else ""
#                     content = HoustonPulseRSSPipeline.clean_text(content_raw)

#                     if apply_pakistan_filter:
#                         if not HoustonPulseRSSPipeline.is_pakistan_related(
#                             title + " " + content
#                         ):
#                             logger.debug(
#                                 f"Not Pakistan-related (pre-filter), skipping: '{title}'"
#                             )
#                             continue

#                     logger.info(f"Full fetch for '{title}' — {link}")
#                     full = HoustonPulseRSSPipeline.full_description(link)

#                     if full["content"] and len(full["content"]) > len(content):
#                         content = full["content"]

#                     image_url = full["image"]
#                     pub_date = full["published"] or rss_pub_date

#                     if len(content) < 200:
#                         logger.info(f"Skipped (too short after full fetch): '{title}'")
#                         continue

#                     # Re-check post-fetch, since the full article body may
#                     # confirm/deny relevance better than the RSS snippet did.
#                     if apply_pakistan_filter:
#                         if not HoustonPulseRSSPipeline.is_pakistan_related(
#                             title + " " + content
#                         ):
#                             logger.debug(
#                                 f"Not Pakistan-related (post-fetch), skipping: '{title}'"
#                             )
#                             continue

#                     articles.append(
#                         {
#                             "id": link,
#                             "article_id": str(uuid.uuid4()),
#                             "articlePubDate": pub_date,
#                             "feedBuildDate": feed_build_date,
#                             "title": title,
#                             "authors": author,
#                             "language": "en-US",
#                             "image": image_url,
#                             "source": HoustonPulseRSSPipeline.SOURCE,
#                             "content": content,
#                             "genre": "News",
#                             "media_origin": "international",
#                             "tags": categories,
#                         }
#                     )

#                     logger.info(
#                         f"Added: '{title}' | {len(content)} chars | "
#                         f"image={'yes' if image_url else 'no'} | "
#                         f"date={'article' if full['published'] else 'rss-fallback'}"
#                     )

#                 except Exception as e:
#                     logger.warning(f"Failed parsing item: {e}")
#                     continue

#             logger.info(f"Parsed {len(articles)} articles from {feed_url}")
#             return articles

#         except Exception as e:
#             logger.error(f"RSS fetch failed [{feed_url}]: {e}")
#             return []

#     @staticmethod
#     def run_pipeline(input_data=None):
#         try:
#             all_articles = []

#             logger.info("── Tier 1: Google News (Pakistani-American + Houston) ──")
#             for feed_url in HoustonPulseRSSPipeline.GOOGLE_NEWS_FEEDS:
#                 all_articles.extend(
#                     HoustonPulseRSSPipeline.fetch_rss_feed(
#                         feed_url, is_google_news=True, apply_pakistan_filter=False
#                     )
#                 )

#             logger.info("── Tier 2: Houston local feeds (Pakistan keyword filter) ──")
#             for feed_url in HoustonPulseRSSPipeline.HOUSTON_LOCAL_FEEDS:
#                 all_articles.extend(
#                     HoustonPulseRSSPipeline.fetch_rss_feed(
#                         feed_url, is_google_news=False, apply_pakistan_filter=True
#                     )
#                 )

#             if not all_articles:
#                 return {"inserted_count": 0, "total_articles": 0}

#             all_articles = list({a["id"]: a for a in all_articles}.values())

#             logger.info(f"After dedupe: {len(all_articles)} total articles")

#             return SupabaseClient.insert_articles_current_year(all_articles)

#         except Exception as e:
#             logger.error(f"Houston Pulse pipeline failed: {e}")
#             return {
#                 "inserted_count": 0,
#                 "total_articles": 0,
#                 "error": str(e),
#             }
