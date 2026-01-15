from http import HTTPStatus
from flask import Blueprint, jsonify
from app.blc.NewsBLC import NewsBLC
from app.extensions import mongo
from webargs.flaskparser import use_args
from webargs import fields

bp = Blueprint("news", __name__)


@bp.route("/get_news", methods=["GET"])
@use_args(
    {
        "sources": fields.List(fields.String(required=False, missing=None)),
        "genres": fields.List(fields.String(required=False, missing=None)),
        "search": fields.String(required=False, missing=None),
        "datetime": fields.DateTime(required=False, missing=None),
        "maxArticles": fields.Integer(required=False, missing=None),
    },
    location="query",
)
def get_news(args: dict):
    try:
        with mongo.db.client.start_session() as session:
            result = NewsBLC.get_filtered_news(args=args, session=session)

    except Exception as e:
        return jsonify({"error": str(e)}), HTTPStatus.UNPROCESSABLE_ENTITY

    return jsonify(result), HTTPStatus.OK


@bp.route("/sources_with_genres", methods=["GET"])
def get_sources_with_genres():
    try:
        with mongo.db.client.start_session() as session:
            sources_with_genres = NewsBLC.get_sources_with_genres(session)
        return jsonify(sources_with_genres), HTTPStatus.OK
    except Exception as e:
        return jsonify({"error": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR
