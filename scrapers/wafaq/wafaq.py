import re
import uuid
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import cloudscraper
from config import WAFAQ_TABLE

from googlenewsdecoder import new_decoderv1
from urllib.parse import urlparse

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class IslamabadRSSPipeline:
    """
    Targeted News Pipeline for Islamabad
    """

    SOURCE = "Rss Feeds"

    MAX_WORKERS = 15

    GOOGLE_NEWS_FEEDS = [
        "https://news.google.com/rss/search?q=Islamabad+when:7d&hl=en-PK&gl=PK&ceid=PK:en",
    ]

    GENERAL_NATIONAL_FEEDS = [
        "https://tribune.com.pk/feed/homepage",
        "https://www.dawn.com/feeds/home",
        "https://www.thenews.com.pk/rss/1/1",
    ]

    ISLAMABAD_KEYWORDS = [
        # General
        "islamabad",
        "ict",
        "islamabad capital territory",
        "federal capital",
        "capital city",
        # CDA / Government
        "cda",
        "capital development authority",
        "capital development authority islamabad",
        "pak secretariat",
        "cabinet division",
        "foreign office",
        "parliament house",
        "president house",
        "prime minister house",
        "constitution avenue",
        # Police & Administration
        "islamabad police",
        "islamabad capital police",
        "ict police",
        "district administration islamabad",
        "deputy commissioner islamabad",
        "dc islamabad",
        "assistant commissioner islamabad",
        # Courts
        "islamabad high court",
        "ihc",
        "supreme court",
        # Roads
        "jinnah avenue",
        "constitution avenue",
        "srinagar highway",
        "kashmir highway",
        "islamabad expressway",
        "margalla avenue",
        # Sectors
        "f-5",
        "f-6",
        "f-7",
        "f-8",
        "f-9",
        "f-10",
        "f-11",
        "f-12",
        "g-5",
        "g-6",
        "g-7",
        "g-8",
        "g-9",
        "g-10",
        "g-11",
        "g-12",
        "g-13",
        "i-8",
        "i-9",
        "i-10",
        "i-11",
        "d-12",
        "e-7",
        "e-8",
        "e-9",
        "e-11",
        "c-13",
        # Areas
        "bhara kahu",
        "bara kahu",
        "nilore",
        "tramri",
        "golra",
        "tarnol",
        "sangjani",
        "rawat",
        "kirpa",
        "lehtrar",
        "chak shahzad",
        # Housing Societies
        "dha islamabad",
        "bahria town islamabad",
        "pwd",
        "gulberg greens",
        "gulberg residences",
        "naval anchorage",
        # Famous Places
        "blue area",
        "red zone",
        "centaurus",
        "faisal mosque",
        "pakistan monument",
        "lok virsa",
        "diplomatic enclave",
        "serena hotel",
        # Tourist Attractions
        "margalla hills",
        "margalla hills national park",
        "pir sohawa",
        "daman-e-koh",
        "shakarparian",
        "saidpur village",
        "bari imam",
        # Airport
        "islamabad international airport",
        "new islamabad airport",
        # Hospitals
        "pims",
        "pakistan institute of medical sciences",
        "polyclinic hospital",
        "federal general hospital",
        "shifa international hospital",
        # Universities
        "nust",
        "national university of sciences and technology",
        "comsats islamabad",
        "international islamic university",
        "iiui",
        "air university",
        "numl",
        "quaid-i-azam university",
        # Metro
        "metro bus",
        "metro station",
        # Parks
        "fatima jinnah park",
        "f-9 park",
        "lake view park",
        "rose and jasmine garden",
        # Religious Places
        "faisal mosque",
        "bari imam",
        # Common References
        "twin cities",
    ]

    SKIP_DOMAINS = {
        "malaysiasun.com",
        "www.malaysiasun.com",
        "app.com.pk",
        "www.app.com.pk",
        "voicepk.net",
        "www.voicepk.net",
        "balochistanpulse.com",
        "www.balochistanpulse.com",
        "ndtv.com",
        "www.ndtv.com",
        "thestatesman.com",
        "www.thestatesman.com",
        "newsonair.gov.in",
        "www.newsonair.gov.in",
        "cricinfo.com",
        "www.cricinfo.com",
    }

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

        # ISO parser for strings like "2026-07-25 04:38:59+05:00"
        try:
            iso_str = date_str.replace(" ", "T")
            dt = datetime.fromisoformat(iso_str)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass

        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y %I:%M %p",
            "%d/%m/%Y %H:%M:%S",
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

        logger.debug(f"Unrecognized date format: {date_str}")
        return None

    @staticmethod
    def extract_published_date(soup):
        for tag_name, attrs in IslamabadRSSPipeline.DATE_META_CANDIDATES:
            tag = soup.find(tag_name, attrs=attrs)
            if tag and tag.get("content"):
                parsed = IslamabadRSSPipeline.parse_date(tag["content"])
                if parsed:
                    return parsed

        time_tag = soup.find("time")
        if time_tag and time_tag.get("datetime"):
            parsed = IslamabadRSSPipeline.parse_date(time_tag["datetime"])
            if parsed:
                return parsed

        return None

    @staticmethod
    def extract_source(soup):
        candidates = [
            ("meta", {"property": "og:site_name"}),
            ("meta", {"name": "application-name"}),
            ("meta", {"name": "publisher"}),
        ]

        for tag_name, attrs in candidates:
            tag = soup.find(tag_name, attrs=attrs)
            if tag and tag.get("content"):
                return tag["content"].strip()

        return None

    @staticmethod
    def site_specific_cleanup(container, domain):
        selectors = []

        if "app.com.pk" in domain:
            selectors = [
                ".jeg_share_button",
                ".jeg_meta_container",
                ".jeg_post_tags",
                ".jeg_postblock",
                ".jeg_ad",
                ".jeg_sidebar",
                ".sharedaddy",
                ".author-box",
            ]

        elif "urdupoint.com" in domain:
            selectors = [
                ".news-author",
                ".news-date",
                ".social-icons",
                ".related-news",
                ".sidebar",
                ".tags",
                ".ads",
                "i[aria-label='Published Time']",
            ]

        elif "tribune.com.pk" in domain:
            selectors = [
                ".story__meta",
                ".story__sidebar",
                ".story__tags",
                ".story__related",
                ".sidebar",
                ".advertisement",
            ]

        elif "dawn.com" in domain:
            selectors = [
                ".story__meta",
                ".story__sidebar",
                ".story__tags",
                ".story__related",
                ".sidebar",
                ".advertisement",
            ]

        elif "thenews.com.pk" in domain:
            selectors = [
                ".newsauthor",
                ".newsdate",
                ".relatednews",
                ".tags",
                ".advertisement",
                ".sidebar",
            ]

        elif "nation.com.pk" in domain:
            selectors = [
                ".author-box",
                ".post-meta",
                ".post-tags",
                ".social-share",
                ".related-posts",
                ".advertisement",
                ".sidebar",
            ]

        for selector in selectors:
            for tag in container.select(selector):
                tag.decompose()

    @staticmethod
    def clean_article_text(text):
        if not text:
            return ""

        patterns = [
            r"Published:\s*.*?(?=\n|$)",
            r"Updated:\s*.*?(?=\n|$)",
            r"Last Updated.*?(?=\n|$)",
            r"By\s+[A-Z][A-Za-z\s]+(?=\n|$)",
            r"Copyright\s+\d{4}.*?All rights reserved\.?",
            r"©\s*\d{4}.*?All rights reserved\.?",
            r"\(AP Photo.*?\)",
            r"Photo by.*?(?=\n|$)",
            r"Image courtesy.*?(?=\n|$)",
        ]

        for pattern in patterns:
            text = re.sub(pattern, "", text, flags=re.IGNORECASE)

        return " ".join(text.split())

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
        if " - " in title:
            return title.rsplit(" - ", 1)[-1].strip()
        return None

    @staticmethod
    def resolve_author(item, title):
        author_elem = item.find("dc:creator") or item.find("author")
        if author_elem and author_elem.get_text(strip=True):
            return author_elem.get_text(strip=True)

        from_title = IslamabadRSSPipeline.extract_author_from_title(title)
        if from_title:
            return from_title

        return None

    @staticmethod
    def resolve_source(item):
        source_elem = item.find("source")
        if source_elem and source_elem.get_text(strip=True):
            return source_elem.get_text(strip=True)

        return IslamabadRSSPipeline.SOURCE

    @staticmethod
    def extract_author(soup):
        candidates = [
            ("meta", {"name": "author"}),
            ("meta", {"property": "author"}),
            ("meta", {"property": "article:author"}),
            ("meta", {"name": "parsely-author"}),
            ("meta", {"name": "twitter:creator"}),
        ]

        for tag_name, attrs in candidates:
            tag = soup.find(tag_name, attrs=attrs)
            if tag and tag.get("content"):
                return tag["content"].strip()

        return None

    @staticmethod
    def is_islamabad_related(text):
        lower = text.lower()
        return any(kw in lower for kw in IslamabadRSSPipeline.ISLAMABAD_KEYWORDS)

    @staticmethod
    def resolve_google_news_link(google_url):
        if not google_url:
            return None
        if "news.google.com" not in google_url:
            return google_url
        try:
            result = new_decoderv1(google_url, interval=0)
            if result.get("status"):
                return result.get("decoded_url")
            logger.debug(f"Google News decode failed: {result.get('message')}")
            return None
        except Exception as e:
            logger.debug(f"Google News decode error for {google_url}: {e}")
            return None

    @staticmethod
    def full_description(link):
        result = {
            "content": "",
            "image": None,
            "published": None,
            "tags": [],
            "author": None,
            "source": None,
        }

        if not link:
            return result

        try:
            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(link, timeout=8, headers=get_random_headers())
                response.raise_for_status()
                html = response.text

            soup = BeautifulSoup(html, "lxml")
            domain = urlparse(link).netloc

            result["image"] = IslamabadRSSPipeline.extract_image(soup)
            result["published"] = IslamabadRSSPipeline.extract_published_date(soup)
            result["tags"] = IslamabadRSSPipeline.extract_tags_from_article(soup)
            result["author"] = IslamabadRSSPipeline.extract_author(soup)
            result["source"] = IslamabadRSSPipeline.extract_source(soup)

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
                    IslamabadRSSPipeline.site_specific_cleanup(container, domain)
                    for p in container.find_all("p"):
                        text = IslamabadRSSPipeline.clean_text(str(p))
                        text = IslamabadRSSPipeline.clean_article_text(text)

                        if len(text) < 30:
                            continue

                        paragraphs.append(text)

                    if paragraphs:
                        break

            if not paragraphs:
                for p in soup.find_all("p"):
                    text = IslamabadRSSPipeline.clean_text(str(p))
                    text = IslamabadRSSPipeline.clean_article_text(text)

                    if len(text) < 30:
                        continue

                    paragraphs.append(text)

            result["content"] = " ".join(paragraphs)

        except Exception as e:
            logger.debug(f"Failed to fetch full article {link}: {e}")

        return result

    @staticmethod
    def process_item(item, is_google_news, apply_islamabad_filter, feed_build_date):
        """
        Process a single <item> from an RSS feed into an article dict (or None
        if it should be skipped). This does the network-bound work (Google
        News link resolution + full article fetch), so it's designed to be
        safely called from a worker thread: it only touches its own local
        variables and the immutable/read-only class data, never shared state.
        """
        try:
            title_elem = item.find("title")
            link_elem = item.find("link")
            pubdate_elem = item.find("articlePubDate")

            if not title_elem or not link_elem:
                return None

            title = title_elem.get_text(strip=True)
            raw_link = link_elem.get_text(strip=True)

            content_elem = item.find("content:encoded") or item.find("description")
            content_raw = content_elem.get_text() if content_elem else ""
            content = IslamabadRSSPipeline.clean_text(content_raw)

            if apply_islamabad_filter:
                if not IslamabadRSSPipeline.is_islamabad_related(title + " " + content):
                    logger.debug(
                        f"Not Islamabad-related (pre-filter), skipping: '{title}'"
                    )
                    return None

            if is_google_news:
                link = IslamabadRSSPipeline.resolve_google_news_link(raw_link)
                if not link:
                    logger.info(
                        f"Could not resolve Google News link, skipping: '{title}'"
                    )
                    return None
            else:
                link = raw_link

            domain = urlparse(link).netloc.lower()
            if domain in IslamabadRSSPipeline.SKIP_DOMAINS:
                logger.info(f"Skipping article from {domain}: '{title}'")
                return None

            rss_pub_date = (
                IslamabadRSSPipeline.parse_date(pubdate_elem.get_text())
                if pubdate_elem
                else None
            ) or datetime.now(timezone.utc)

            source = IslamabadRSSPipeline.resolve_source(item)

            rss_categories = [
                c.get_text(strip=True)
                for c in item.find_all("category")
                if c.get_text(strip=True)
            ]

            logger.info(f"Processing: '{title}' | source={source} | link={link}")

            full = IslamabadRSSPipeline.full_description(link)

            author = full["author"] or IslamabadRSSPipeline.resolve_author(item, title)
            source = full["source"] or source

            if full["content"] and len(full["content"]) > len(content):
                content = full["content"]

            image_url = full["image"]
            pub_date = full["published"] or rss_pub_date

            seen = set()
            categories = []
            for tag in rss_categories + full["tags"]:
                key = tag.lower()
                if key not in seen:
                    seen.add(key)
                    categories.append(tag)

            if len(content) < 200:
                logger.info(f"Skipped (too short after full fetch): '{title}'")
                return None

            if apply_islamabad_filter:
                if not IslamabadRSSPipeline.is_islamabad_related(title + " " + content):
                    logger.debug(
                        f"Not Islamabad-related (post-fetch), skipping: '{title}'"
                    )
                    return None

            article = {
                "id": link,
                "article_id": str(uuid.uuid4()),
                "articlePubDate": pub_date,
                "feedBuildDate": feed_build_date,
                "title": title,
                "authors": author,
                "language": "en-US",
                "image": image_url,
                "source": source,
                "content": content,
                "genre": "General News",
                "media_origin": "local",
                "tags": categories,
            }

            logger.info(
                f"Added: '{title}' | {len(content)} chars | "
                f"author={author} | tags={len(categories)} | "
                f"image={'yes' if image_url else 'no'} | "
                f"date={'article' if full['published'] else 'rss-fallback'}"
            )

            return article

        except Exception as e:
            logger.warning(f"Failed parsing item: {e}")
            return None

    @staticmethod
    def fetch_rss_feed(
        feed_url,
        is_google_news,
        apply_islamabad_filter=True,
    ):
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
            items = soup.find_all("item")
            feed_build_date = datetime.now(timezone.utc)
            articles = []

            # Each item requires its own network round-trip(s) (Google News
            # decode + full article fetch), so this is I/O bound work — a
            # thread pool gives a real speedup here without the complexity
            # of multiprocessing, since threads release the GIL while
            # waiting on network calls.
            with ThreadPoolExecutor(
                max_workers=IslamabadRSSPipeline.MAX_WORKERS
            ) as executor:
                futures = [
                    executor.submit(
                        IslamabadRSSPipeline.process_item,
                        item,
                        is_google_news,
                        apply_islamabad_filter,
                        feed_build_date,
                    )
                    for item in items
                ]

                for future in as_completed(futures):
                    try:
                        article = future.result()
                    except Exception as e:
                        logger.warning(f"Worker failed on item: {e}")
                        continue

                    if article is not None:
                        articles.append(article)

            logger.info(f"Parsed {len(articles)} articles from {feed_url}")
            return articles

        except Exception as e:
            logger.error(f"RSS fetch failed [{feed_url}]: {e}")
            return []

    @staticmethod
    def run_pipeline(input_data=None, table_name=None):
        target_table = table_name or WAFAQ_TABLE
        try:
            all_articles = []

            logger.info("── Islamabad, Pakistan — Google News ──")
            for feed_url in IslamabadRSSPipeline.GOOGLE_NEWS_FEEDS:
                all_articles.extend(
                    IslamabadRSSPipeline.fetch_rss_feed(
                        feed_url,
                        is_google_news=True,
                        apply_islamabad_filter=True,
                    )
                )

            logger.info("── Islamabad, Pakistan — General National Feeds ──")
            for feed_url in IslamabadRSSPipeline.GENERAL_NATIONAL_FEEDS:
                all_articles.extend(
                    IslamabadRSSPipeline.fetch_rss_feed(
                        feed_url,
                        is_google_news=False,
                        apply_islamabad_filter=True,
                    )
                )

            if not all_articles:
                return {"inserted_count": 0, "total_articles": 0}

            all_articles = list({a["id"]: a for a in all_articles}.values())

            logger.info(f"After dedupe: {len(all_articles)} total articles")

            SupabaseClient.delete_old_articles(table_name=target_table)
            logger.info(
                f"Deleted articles older than 7 days from Supabase table '{target_table}'"
            )

            return SupabaseClient.insert_system_articles(
                "wafaq", all_articles, table_name=target_table
            )

        except Exception as e:
            logger.error(f"Islamabad Pulse pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
