from datetime import datetime, timezone
from bs4 import BeautifulSoup
from app.reutersblc.reuters_topics import TOPICS_BY_GENRE
from concurrent.futures import ThreadPoolExecutor
from requests.auth import HTTPDigestAuth
from app.db import MongoDBAsyncClient
from config import settings
from urllib.parse import quote
import requests
import uuid
import time


class ReutersRSSPipeline:

    MAX_THREADS = 5
    SOURCE = "Reuters"
    USERNAME = settings.REUTERS_USERNAME
    PASSWORD = settings.REUTERS_PASSWORD

    @staticmethod
    async def run_pipeline(input_data):
        try:
            start_time = time.time()
            cleaned_data_list = ReutersRSSPipeline.process_input(
                input_data, field="topic"
            )

            mongo_client = MongoDBAsyncClient()
            inserted_count = await mongo_client.insert_documents_with_retry(
                document_list=cleaned_data_list
            )
            end_time = time.time()
            duration = end_time - start_time

            print(
                f"Inserted {inserted_count} documents into MongoDB in {duration:.2f} seconds."
            )

        except Exception as e:
            print(f"Error: {e}")

    @staticmethod
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

        return f"{base_url}?{'&'.join(params)}"

    @staticmethod
    def parse_date(date_str):
        try:
            dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    @staticmethod
    def fetch_reuters_rss_feed(topic_code, field="topic", channel=None, genre_key=None):
        max_retries = 1
        for attempt in range(max_retries + 1):
            try:
                url = ReutersRSSPipeline.build_basic_search_url(
                    topic_code, field=field, channel=channel
                )
                response = requests.get(
                    url,
                    auth=HTTPDigestAuth(
                        ReutersRSSPipeline.USERNAME, ReutersRSSPipeline.PASSWORD
                    ),
                    timeout=60,
                )
                if response.status_code == 200:
                    root = BeautifulSoup(response.content, "xml")
                    items = root.find_all("item")
                    result = []

                    feed_build_date_str = root.find("lastBuildDate")
                    feed_build_date = (
                        ReutersRSSPipeline.parse_date(feed_build_date_str.text)
                        if feed_build_date_str
                        else None
                    )

                    for item in items:
                        pub_date_str = item.find("pubDate")
                        article_pub_date = (
                            ReutersRSSPipeline.parse_date(pub_date_str.text)
                            if pub_date_str
                            else None
                        )
                        content = ""
                        media_text = item.find("media:text")
                        if media_text and media_text.get("type") == "html":
                            content_html = media_text.text or ""
                            content = (
                                BeautifulSoup(content_html, "html.parser")
                                .get_text(separator=" ")
                                .strip()
                            )

                        if not content or len(content.split()) < 200:
                            continue

                        article = {
                            "_id": (
                                item.find("link").text
                                if item.find("link")
                                else str(uuid.uuid4())
                            ),
                            "article_id": str(uuid.uuid4()),
                            "articlePubDate": article_pub_date,
                            "feedBuildDate": feed_build_date,
                            "title": (
                                item.find("title").text if item.find("title") else ""
                            ),
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
                            "source": ReutersRSSPipeline.SOURCE,
                            "content": content,
                            "genre": genre_key or "Unknown",
                            "media_origin": "foreign",
                            "tags": [
                                c.text for c in item.find_all("category") if c.text
                            ],
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

    @staticmethod
    def fetch_single_topic(args):
        topic_code, field, source, genre_key = args
        try:
            return ReutersRSSPipeline.fetch_reuters_rss_feed(
                topic_code, field, source, genre_key
            )
        except Exception as e:
            print(f"Error fetching topic '{topic_code}' for genre '{genre_key}': {e}")
            return []

    @staticmethod
    def process_input(input_data, field="topic"):
        if not isinstance(input_data, dict) or sorted(input_data.keys()) != ["genres"]:
            raise ValueError("Input must contain only 'genres' key as a list.")

        all_args = []
        for genre_key in input_data["genres"]:
            topic_entries = TOPICS_BY_GENRE.get(genre_key)
            if not topic_entries:
                print(f"Genre '{genre_key}' not found.")
                continue
            for topic_dict in topic_entries:
                for topic_code in topic_dict:
                    print(f"Processing genre '{genre_key}' with topic '{topic_code}'")
                    all_args.append(
                        (topic_code, field, ReutersRSSPipeline.SOURCE, genre_key)
                    )

        all_results = []
        with ThreadPoolExecutor(max_workers=ReutersRSSPipeline.MAX_THREADS) as executor:
            results = executor.map(ReutersRSSPipeline.fetch_single_topic, all_args)
            for articles in results:
                if articles:
                    all_results.extend(articles)

        return all_results
