import time
import re
import logging
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import cloudscraper
from urllib.parse import urlparse
from app.utilities import get_random_headers
from googlenewsdecoder import new_decoderv1

logger = logging.getLogger(__name__)

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


def parse_date(date_str):
    if not date_str:
        return None
    date_str = date_str.strip().replace("UT", "UTC")
    try:
        iso_str = date_str.replace(" ", "T")
        dt = datetime.fromisoformat(iso_str)
        return (
            dt.astimezone(timezone.utc)
            if dt.tzinfo
            else dt.replace(tzinfo=timezone.utc)
        )
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
    return None


def extract_published_date(soup):
    for tag_name, attrs in DATE_META_CANDIDATES:
        tag = soup.find(tag_name, attrs=attrs)
        if tag and tag.get("content"):
            parsed = parse_date(tag["content"])
            if parsed:
                return parsed

    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        parsed = parse_date(time_tag["datetime"])
        if parsed:
            return parsed
    return None


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

    for sel in ["article", "[class*='article-body']", "[class*='story-body']", "main"]:
        container = soup.select_one(sel)
        if container:
            img = container.find("img")
            if img:
                src = img.get("src") or img.get("data-src")
                if src and src.startswith("http"):
                    return src
    return None


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
            "p.yrzypdyuqs",
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
    elif "dailypakistan.com.pk" in domain:
        selectors = [
            ".author-box",
            ".post-meta",
            ".post-tags",
            ".social-share",
            ".related-posts",
            ".advertisement",
            ".sidebar",
        ]
    elif "pakobserver.net" in domain:
        selectors = [
            ".post-meta",
            ".author-box",
            ".post-tags",
            ".share-buttons",
            ".related-posts",
            ".advertisement",
            ".sidebar",
        ]

    for selector in selectors:
        for tag in container.select(selector):
            tag.decompose()


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
        return " ".join(re.sub(r"http\S+|www\.\S+", "", text).split())
    except Exception as e:
        logger.warning(f"Content cleaning failed: {e}")
        return html or ""


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


def extract_author_from_title(title):
    if " - " in title:
        return title.rsplit(" - ", 1)[-1].strip()
    return None


def resolve_author(item, title):
    author_elem = item.find("dc:creator") or item.find("author")
    if author_elem and author_elem.get_text(strip=True):
        return author_elem.get_text(strip=True)
    from_title = extract_author_from_title(title)
    if from_title:
        return from_title
    return None


def resolve_source(item, fallback_source="Google News"):
    source_elem = item.find("source")
    if source_elem and source_elem.get_text(strip=True):
        return source_elem.get_text(strip=True)
    return fallback_source


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


def resolve_google_news_link(google_url):
    if not google_url:
        return None
    if "news.google.com" not in google_url:
        return google_url
    try:
        time.sleep(10)
        result = new_decoderv1(google_url, interval=0)
        if result.get("status"):
            return result.get("decoded_url")
        logger.debug(f"Google News decode failed: {result.get('message')}")
        return None
    except Exception as e:
        logger.debug(f"Google News decode error for {google_url}: {e}")
        return None


def full_description(link):
    result = {
        "content": "",
        "image": None,
        "published": None,
        "tags": [],
        "author": None,
        "genre": "General News",
    }
    if not link:
        return result
    try:
        with cloudscraper.create_scraper() as scraper:
            response = scraper.get(link, timeout=8, headers=get_random_headers())
            response.raise_for_status()
            html = response.text

        soup = BeautifulSoup(html, "lxml")
        domain = urlparse(link).netloc.lower()

        result["image"] = extract_image(soup)
        result["published"] = extract_published_date(soup)
        result["tags"] = extract_tags_from_article(soup)
        result["author"] = extract_author(soup)

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
                site_specific_cleanup(container, domain)
                for p in container.find_all("p"):
                    text = clean_text(str(p))
                    text = clean_article_text(text)
                    if len(text) < 30:
                        continue
                    paragraphs.append(text)
                if paragraphs:
                    break

        if not paragraphs:
            for p in soup.find_all("p"):
                text = clean_text(str(p))
                text = clean_article_text(text)
                if len(text) < 30:
                    continue
                paragraphs.append(text)

        result["content"] = " ".join(paragraphs)
    except Exception as e:
        logger.debug(f"Failed to fetch full article {link}: {e}")

    return result
