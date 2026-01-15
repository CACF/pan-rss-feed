from flask_pymongo import PyMongo


mongo = PyMongo()


def build_db_uri(
    DB_USER: str = None,
    DB_PW: str = None,
    DB_HOST: str = None,
    DB_PORT: int = None,
    DB_NAME: str = None,
) -> str:
    return "mongodb://{}:{}@{}:{}/{}?authSource=admin".format(
        DB_USER, DB_PW, DB_HOST, DB_PORT, DB_NAME
    )
