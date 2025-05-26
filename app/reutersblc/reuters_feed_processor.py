from datetime import datetime, timezone
from bs4 import BeautifulSoup
from app.reutersblc.reuters_topics import TOPICS_BY_GENRE
from concurrent.futures import ThreadPoolExecutor
from requests.auth import HTTPDigestAuth
from app.db import MongoDBAsyncClient
from dotenv import load_dotenv
from urllib.parse import quote
import requests
import uuid
import time
import os

load_dotenv()
USERNAME = os.getenv("REUTERS_USERNAME")
PASSWORD = os.getenv("REUTERS_PASSWORD")


MAX_THREADS = 10
SOURCE = "Reuters"


async def run_reuters_feed_pipeline(input_data):
    try:
        cleaned_data_list = process_input(input_data, field="topic")
        mongo_client = MongoDBAsyncClient()
        await mongo_client.insert_documents_with_retry(document_list=cleaned_data_list)
        print(f"✅ Inserted {len(cleaned_data_list)} documents into MongoDB.")
    except Exception as e:
        print(f"❌ Error: {e}")


def build_basic_search_url(
    search_term,
    field="topic",
    channel=None,
    channel_categories=["TXT", "OLR"],
    limit=100,
    max_age="7D",
    link_type="raw",
):
    base_url = "http://rmb.reuters.com/rmd/rss/basicSearch"
    params = [f"q={quote(search_term)}"]
    if field:
        params.append(f"field={field}")
    if channel:
        params.append(f"channel={quote(channel)}")
    for category in channel_categories:
        params.append(f"channelCategory={category}")
    params.append(f"limit={limit}")
    params.append(f"maxAge={max_age}")
    params.append(f"linkType={link_type}")
    # print(f"Channel: {channel}, Topic: {search_term}")
    return f"{base_url}?{'&'.join(params)}"


def parse_date(date_str):
    try:
        dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def fetch_reuters_rss_feed(topic_code, field="topic", channel=None, genre_key=None):
    max_retries = 1
    for attempt in range(max_retries + 1):
        try:
            url = build_basic_search_url(topic_code, field=field, channel=channel)
            response = requests.get(
                url, auth=HTTPDigestAuth(USERNAME, PASSWORD), timeout=60
            )
            if response.status_code == 200:
                root = BeautifulSoup(response.content, "xml")
                items = root.find_all("item")
                # print(f"Fetched {len(items)} items for topic '{topic_code}'.")
                result = []

                feed_build_date_str = root.find("lastBuildDate")
                feed_build_date = (
                    parse_date(feed_build_date_str.text)
                    if feed_build_date_str
                    else None
                )

                for item in items:
                    pub_date_str = item.find("pubDate")
                    article_pub_date = (
                        parse_date(pub_date_str.text) if pub_date_str else None
                    )

                    # Extract content from <media:text type="html"> or fallback to <description>
                    media_text = item.find("media:text")
                    if media_text and media_text.get("type") == "html":
                        content_html = media_text.text or ""
                        content = (
                            BeautifulSoup(content_html, "html.parser")
                            .get_text(separator=" ")
                            .strip()
                        )
                    else:
                        desc_tag = item.find("description")
                        content = (
                            desc_tag.text.strip() if desc_tag and desc_tag.text else ""
                        )

                    if not content:
                        continue  # Skip if no content is found

                    article = {
                        "_id": (
                            item.find("link").text
                            if item.find("link")
                            else str(uuid.uuid4())
                        ),
                        "article_id": str(uuid.uuid4()),
                        "articlePubDate": article_pub_date,
                        "feedBuildDate": feed_build_date,
                        "title": item.find("title").text if item.find("title") else "",
                        "authors": (
                            item.find("dc:creator").text
                            if item.find("dc:creator")
                            else "Unknown"
                        ),
                        "language": (
                            item.find("dc:language").text
                            if item.find("dc:language")
                            else "en-us"
                        ),
                        "source": "Reuters",
                        "content": content,
                        "genre": genre_key or "Unknown",
                        "media_origin": "foreign",
                        "tags": [c.text for c in item.find_all("category") if c.text],
                    }

                    result.append(article)

                return result
            elif response.status_code == 500:
                time.sleep(1)
                continue
            else:
                return []
        except Exception as e:
            print(f"Error fetching topic '{topic_code}': {e}")
            return []


def fetch_single_topic(args):
    """Helper function to unpack args and fetch articles."""
    topic_code, field, source, genre_key = args
    print(f"Fetching topic '{topic_code}' for genre '{genre_key}'...")
    try:
        return fetch_reuters_rss_feed(topic_code, field, source, genre_key)
    except Exception as e:
        print(f"❌ Error fetching topic '{topic_code}' for genre '{genre_key}': {e}")
        return []


def process_input(input_data, field="topic"):
    """
    Process input dict with only 'genres', fetch and aggregate articles concurrently.

    input_data example:
    {
        "genres": ["Arts/Culture/Entertainment", "Business - Assets"]
    }
    """
    if not isinstance(input_data, dict) or sorted(input_data.keys()) != ["genres"]:
        raise ValueError("Input must contain only 'genres' key as a list.")

    all_args = []

    for genre_key in input_data["genres"]:
        topic_entries = TOPICS_BY_GENRE.get(genre_key)
        if not topic_entries:
            print(f"⚠️ Genre '{genre_key}' not found.")
            continue

        for topic_dict in topic_entries:
            for topic_code in topic_dict:
                all_args.append((topic_code, field, SOURCE, genre_key))

    all_results = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        results = executor.map(fetch_single_topic, all_args)
        for articles in results:
            if articles:
                all_results.extend(articles)

    return all_results
