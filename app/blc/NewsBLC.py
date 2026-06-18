from marshmallow import ValidationError
from app.repositories.NewsRepository import News
from pymongo.client_session import ClientSession
from app.extensions import mongo


class NewsBLC:
    @staticmethod
    def get_filtered_news(args: dict, session: ClientSession) -> dict:
        allowed_sources = list(mongo.db["News"].distinct("source"))

        requested_sources = args.get("sources", [])
        if requested_sources:
            requested_sources_clean = [src.strip().lower() for src in requested_sources]
            allowed_sources_clean = [src.strip().lower() for src in allowed_sources]

            invalid_sources = [
                src
                for src in requested_sources_clean
                if src not in allowed_sources_clean
            ]

            if invalid_sources:
                raise ValidationError(
                    f"You have not subscribed to the following sources: {', '.join(invalid_sources)}"
                )

            filtered_sources = [
                s
                for s in allowed_sources
                if s.strip().lower() in requested_sources_clean
            ]
        else:
            filtered_sources = allowed_sources

        max_articles = args.get("maxArticles")

        genres = args.get("genres")
        if genres:
            if isinstance(genres, list):
                genres = [g.strip() for g in genres]
            else:
                genres = [genres.strip()]

        news = News.filter_documents(
            session=session,
            search=args.get("search"),
            sources=filtered_sources,
            genres=genres,
            filter_datetime=args.get("datetime"),
            maxArticles=max_articles,
        )

        return news

    @staticmethod
    def get_sources_with_genres(session):
        sources_with_genres = {}
        collection_name = "News"

        # Get distinct sources directly from News collection
        distinct_sources = list(mongo.db[collection_name].distinct("source"))

        for source in distinct_sources:
            if not source:
                continue

            source_upper = source.strip().upper()
            if source_upper == "APP NEWS":
                sources_with_genres["APP NEWS"] = News.get_appnews_genres(session)
            else:
                # Generic: get all genres used for this source
                genres = mongo.db[collection_name].distinct("genre", {"source": source})
                genres = [g.strip() for g in genres if g]
                genres.append("General")
                sources_with_genres[source] = sorted(set(genres))

        return sources_with_genres
