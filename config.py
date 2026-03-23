import os
from dotenv import load_dotenv

load_dotenv()

#Remote Connection Parmaters
REMOTE_IP = os.getenv("REMOTE_IP")
REMOTE_PORT = int(os.getenv("REMOTE_PORT"))
MASTER_ADDR = int(os.getenv("MASTER_ADDR"))

# Solid Pod Configuration
SOLID_SERVER = os.getenv("SOLID_SERVER")
RESOURCE_URL = os.getenv("RESOURCE_URL")
COMMANDS_URL = os.getenv("COMMANDS_URL")
OIDC_ISSUER = os.getenv("OIDC_ISSUER")

# CSS Account Credentials
CSS_EMAIL = os.getenv("CSS_EMAIL")
CSS_PASSWORD = os.getenv("CSS_PASSWORD")