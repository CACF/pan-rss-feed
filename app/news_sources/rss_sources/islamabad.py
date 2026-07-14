import re
import uuid
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper

from googlenewsdecoder import new_decoderv1
from urllib.parse import urlparse

from app.utilities import get_random_headers
from app.utils.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class IslamabadRSSPipeline:
    """
    Islamabad Pulse — Targeted News Pipeline for Islamabad
    """

    SOURCE = "Rss Feeds"

    # Google News query feeds already scoped to Islamabad via the search query.
    GOOGLE_NEWS_FEEDS = [
        "https://news.google.com/rss/search?q=Islamabad+when:7d&hl=en-PK&gl=PK&ceid=PK:en",
    ]

    GENERAL_NATIONAL_FEEDS = [
        # "https://tribune.com.pk/feed/homepage",
        # "https://www.dawn.com/feeds/home",
        # "https://www.thenews.com.pk/rss/1/1",
        # "https://www.nation.com.pk/rss/newspaper",
    ]

    ISLAMABAD_KEYWORDS = [
        "islamabad",
        "ict",
        "cda",
        "capital development authority",
        # Sectors
        "f-5",
        "f-6",
        "f-7",
        "f-8",
        "f-9",
        "f-10",
        "f-11",
        "g-5",
        "g-6",
        "g-7",
        "g-8",
        "g-9",
        "g-10",
        "g-11",
        "g-13",
        "i-8",
        "i-9",
        "i-10",
        "i-11",
        "d-12",
        "e-11",
        "e-7",
        "e-8",
        "e-9",
        # Places
        "blue area",
        "red zone",
        "constitution avenue",
        "parliament house",
        "supreme court",
        "faisal mosque",
        "centaurus",
        "pak secretariat",
        "serena hotel",
        "diplomatic enclave",
        # Tourist places
        "margalla hills",
        "pir sohawa",
        "daman-e-koh",
        "shakarparian",
        "saidpur village",
        "bari imam",
        "lok virsa",
        "pakistan monument",
        # Areas
        "nilore",
        "bhara kahu",
        "bara kahu",
        "tramri",
        "golra",
        "rawat",
        "tarnol",
        "sangjani",
        # Housing
        "dha islamabad",
        "bahria town islamabad",
        "pwd",
        # Government
        "islamabad police",
        "islamabad capital police",
        "ict police",
        "district administration islamabad",
        "deputy commissioner islamabad",
        # Common names
        "twin cities",
        "rawalpindi islamabad",
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
    def extract_genre(soup, rss_categories=None):
        rss_categories = rss_categories or []

        if rss_categories:
            return rss_categories[0]

        for attrs in [
            {"property": "article:section"},
            {"name": "section"},
            {"name": "category"},
            {"property": "og:section"},
        ]:
            tag = soup.find("meta", attrs=attrs)
            if tag and tag.get("content"):
                return tag["content"].strip()

        return "General News"

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
        result = {
            "content": "",
            "image": None,
            "published": None,
            "tags": [],
            "author": None,
            "source": None,
            "genre": None,
        }

        if not link:
            return result

        try:
            with cloudscraper.create_scraper() as scraper:
                response = scraper.get(link, timeout=30, headers=get_random_headers())
                response.raise_for_status()
                html = response.text

            soup = BeautifulSoup(html, "lxml")
            domain = urlparse(link).netloc

            result["image"] = IslamabadRSSPipeline.extract_image(soup)
            result["published"] = IslamabadRSSPipeline.extract_published_date(soup)
            result["tags"] = IslamabadRSSPipeline.extract_tags_from_article(soup)
            result["author"] = IslamabadRSSPipeline.extract_author(soup)
            result["source"] = IslamabadRSSPipeline.extract_source(soup)
            result["genre"] = IslamabadRSSPipeline.extract_genre(soup)

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
            logger.warning(f"Failed to fetch full article {link}: {e}")

        return result

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

            for item in items:
                try:
                    title_elem = item.find("title")
                    link_elem = item.find("link")
                    pubdate_elem = item.find("articlePubDate")

                    if not title_elem or not link_elem:
                        continue

                    title = title_elem.get_text(strip=True)
                    raw_link = link_elem.get_text(strip=True)

                    if is_google_news:
                        link = IslamabadRSSPipeline.resolve_google_news_link(raw_link)
                        if not link:
                            logger.info(
                                f"Could not resolve Google News link, skipping: '{title}'"
                            )
                            continue
                    else:
                        link = raw_link

                    domain = urlparse(link).netloc.lower()
                    if "urdupoint.com" in domain or "malaysiasun" in domain:
                        logger.info(f"Skipping UrduPoint article: '{title}'")
                        continue

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

                    content_elem = item.find("content:encoded") or item.find(
                        "description"
                    )
                    content_raw = content_elem.get_text() if content_elem else ""
                    content = IslamabadRSSPipeline.clean_text(content_raw)

                    if apply_islamabad_filter:
                        if not IslamabadRSSPipeline.is_islamabad_related(
                            title + " " + content
                        ):
                            logger.debug(
                                f"Not Islamabad-related (pre-filter), skipping: '{title}'"
                            )
                            continue

                    logger.info(
                        f"Processing: '{title}' | source={source} | link={link}"
                    )

                    full = IslamabadRSSPipeline.full_description(link)

                    author = full["author"] or IslamabadRSSPipeline.resolve_author(
                        item, title
                    )
                    source = full["source"] or source

                    if full["content"] and len(full["content"]) > len(content):
                        content = full["content"]

                    image_url = full["image"]
                    pub_date = full["published"] or rss_pub_date

                    genre = (
                        rss_categories[0]
                        if rss_categories
                        else (full["genre"] or "General News")
                    )

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

                    if apply_islamabad_filter:
                        if not IslamabadRSSPipeline.is_islamabad_related(
                            title + " " + content
                        ):
                            logger.debug(
                                f"Not Islamabad-related (post-fetch), skipping: '{title}'"
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
                            "source": source,
                            "content": content,
                            "genre": genre,
                            "media_origin": "local",
                            "tags": categories,
                        }
                    )

                    logger.info(
                        f"Added: '{title}' | {len(content)} chars | "
                        f"author={author} | genre={genre} | tags={len(categories)} | "
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

            return SupabaseClient.insert_articles_current_year(all_articles)

        except Exception as e:
            logger.error(f"Islamabad Pulse pipeline failed: {e}")
            return {
                "inserted_count": 0,
                "total_articles": 0,
                "error": str(e),
            }
