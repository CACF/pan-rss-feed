import os
from time import time
from app.utils.feed_urls import feed_urls
from app.utils.feed_utilities import FeedParser, MongoDBClient
import re

# Function to check if the content meets the criteria
def is_valid_content(data_obj):
    content = data_obj.get("content")
    content_link = data_obj.get("_id")
    # Remove links from content
    if content:
        content = re.sub(r'http\S+|www\S+|https\S+', '', content, flags=re.MULTILINE)
    if not content or content.isspace() or "https://cricketpakistan.com.pk" in content_link:
        return False
    word_count = len(content.split())
    return word_count >= 20

def feed_starter(data_dict):
    total_time = 0
    source_start_time = time()
    print("Feed Ingestion has started !!!!\n\n")
    sources_to_extract = (
        feed_urls if "all" in data_dict.get("sources") else data_dict.get("sources", [])
    )
    for source_name in sources_to_extract:
        feed = feed_urls.get(source_name)
        media_origin = feed.get("media_origin", "Unknown")
        feed_with_content = feed.get("feed_with_content", False)
        if "urls" in feed:
            for url_dict in feed["urls"]:
                if not data_dict.get("genres") or url_dict.get(
                    "genre", None
                ) in data_dict.get("genres", []):
                    genre_start_time = time()
                    url = url_dict.get("url")
                    genre = url_dict.get("genre")

                    data_list = FeedParser.rss_feeds(
                        media_origin, source_name, genre, url, feed_with_content
                    )
                    # Filter out objects based on the content criteria
                    cleaned_data_list = [data_obj for data_obj in data_list if is_valid_content(data_obj)]
                    # Insert documents via Supabase-backed client
                    mongo_client = MongoDBClient()

                    # Inserting documents into Supabase
                    _ = mongo_client.insert_documents(
                        "news", cleaned_data_list
                    )

                    genre_stop_time = time()
                    genre_time = genre_stop_time - genre_start_time

                    print(
                        "\n\n------------------------------------------------------------------------------"
                    )
                    print(
                        f'Feed of "{source_name}", "{genre.upper()}" completed in {round(genre_time, 2)} seconds'
                    )
                    print(
                        "------------------------------------------------------------------------------\n\n"
                    )

        source_stop_time = time()
        source_time = source_stop_time - source_start_time
        total_time += source_time

        print(
            "\n\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        )
        print(
            f'Ingestion of "{source_name}" all genres completed in {round(source_time, 2)} seconds'
        )
        print(
            "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n"
        )

    else:
        print("Failed to fetch XML data. Please check the Feed Links/URLs")

    print("\n\n######################################################################")
    print(f"Ingestion of all Media-Feed completed in {round(total_time, 2)} seconds")
    print("######################################################################\n\n")

    return True
