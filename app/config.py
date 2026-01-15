import os
SECRET_KEY = os.environ.get("SECRET_KEY")
PUBLIC_KEY_PATH = os.environ.get("PUBLIC_KEY_PATH")
LICENSE_KEY_PATH = os.environ.get("LICENSE_KEY_PATH")
CLIENT_ID = os.environ.get("CLIENT_ID")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
KEYCLOAK_SERVER_URL = os.environ.get("KEYCLOAK_SERVER_URL")
KEYCLOAK_CLIENT_ID = os.environ.get("KEYCLOAK_CLIENT_ID")
KEYCLOAK_REALM_NAME = os.environ.get("KEYCLOAK_REALM_NAME")
RABBIT_MQ = os.environ.get("RABBIT_MQ")
TOGETHER_API_KEY = os.environ.get("TOGETHER_API_KEY")
STRIPE_KEY = os.environ.get("STRIPE_KEY")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")
DB_CREDENTIALS = {
    "DB_HOST": os.environ.get("DB_HOST"),
    "DB_PORT": os.environ.get("DB_PORT"),
    "DB_USER": os.environ.get("DB_USER"),
    "DB_PW": os.environ.get("DB_PW"),
    "DB_NAME": os.environ.get("DB_NAME"),
}
