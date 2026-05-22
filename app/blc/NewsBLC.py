from marshmallow import ValidationError
from app.repositories.NewsRepository import News


class NewsBLC:
    @staticmethod
    def get_filtered_news(args: dict, session=None) -> dict:
        allowed_sources = News.get_distinct_sources()

        requested_sources = args.get("sources", [])
        if requested_sources:
            requested_sources_clean = [src.strip().lower() for src in requested_sources]
            allowed_sources_clean = [src.strip().lower() for src in allowed_sources]

            invalid_sources = [
                src for src in requested_sources_clean if src not in allowed_sources_clean
            ]

            if invalid_sources:
                raise ValidationError(
                    f"You have not subscribed to the following sources: {', '.join(invalid_sources)}"
                )

            filtered_sources = [
                s for s in allowed_sources if s.strip().lower() in requested_sources_clean
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
    def get_sources_with_genres(session=None):
        sources_with_genres = {}

        distinct_sources = News.get_distinct_sources()

        for source in distinct_sources:
            if not source:
                continue

            source_upper = source.strip().upper()
            if source_upper == "APP NEWS":
                sources_with_genres["APP NEWS"] = News.get_appnews_genres(session)
            else:
                genres = News.get_distinct_genres_for_source(source)
                genres.append("General")
                sources_with_genres[source] = sorted(set(genres))

        return sources_with_genres
