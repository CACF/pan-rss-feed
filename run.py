from app.reutersblc.reuters_feed_processor import ReutersRSSPipeline
import uvicorn
from fastapi import Body
from mangum import Mangum
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from app.reutersblc.reuters_topics import TOPICS_BY_GENRE
from typing import Dict, Any, List, Optional, Dict
from app.utils.feed_urls import feed_urls
from app.feed_blc import feed_starter
from fastapi.middleware.cors import CORSMiddleware
from config import settings

app = FastAPI()


origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

handler = Mangum(app)


@app.get("/")
def home():
    return {"api_version": "1.0"}


@app.get("/sources")
def get_sources():
    sources = list(feed_urls.keys())
    return {"sources": sources}


@app.get("/genres")
def get_genres():
    all_genres = set()
    for single_source in feed_urls.values():
        urls_list = single_source.get("urls")
        for single_url in urls_list:
            all_genres.add(single_url.get("genre"))

    return {"genres": list(all_genres)}


@app.get("/reuters/genres")
def get_reuters_topics(
    sources: Optional[List[str]] = Query(default=["Reuters"]),
    genres: Optional[List[str]] = Query(default=None),
) -> Dict[str, Any]:
    if "reuters" not in [source.lower() for source in sources]:
        return {"error": "Only 'Reuters' source is supported."}

    if not genres:
        return TOPICS_BY_GENRE

    return {
        genre: TOPICS_BY_GENRE[genre] for genre in genres if genre in TOPICS_BY_GENRE
    }


@app.post("/reuters")
async def start_reuters_feed(data_dict: dict = Body(..., example={"genres": []})):
    try:
        genres = data_dict.get("genres")
        if not genres:
            genres = list(TOPICS_BY_GENRE.keys())

        elif isinstance(genres, str):
            genres = [genres]

        await ReutersRSSPipeline.run_pipeline({"genres": genres})

        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Reuters feed ingested successfully for genres: {genres}",
            },
        )
    except Exception as ex:
        return JSONResponse(
            status_code=500,
            content={"status": "failed", "error": str(ex)},
        )


@app.post("/ingest")
def start_feed(data_dict: dict = Body(..., example={"sources": [], "genres": []})):
    try:
        if not data_dict.get("sources", None):
            return JSONResponse(
                {"status": "failed", "Error": "Please provide valid sources."},
            )
        feed_starter(data_dict)
        return JSONResponse(
            {"status": "success"},
        )
    except Exception as ex:
        return JSONResponse(
            {"status": "failed", "Error": str(ex)},
        )


@app.get("/sources-with-genres")
def get_sources_with_genres():
    sources_with_genres = {}
    for source, details in feed_urls.items():
        # Initialize the source entry with an empty list
        sources_with_genres[source] = []
        genre_set = set()
        # Extract city names from the set, assuming each set contains only one city name
        for genres in details["urls"]:
            # Add the city name to the source's list
            if genres["genre"] not in genre_set:
                sources_with_genres[source].append(genres["genre"])
                genre_set.add(genres["genre"])
    return sources_with_genres


if __name__ == "__main__":
    uvicorn.run(
        "run:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.ENVIRONMENT == "development",
    )
