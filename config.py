import os
from dotenv import load_dotenv

load_dotenv()

# Modbus Configuration
MODBUS_HOST = os.getenv("MODBUS_HOST", "localhost")
MODBUS_PORT = int(os.getenv("MODBUS_PORT", 5020))
REGISTER_COUNT = int(os.getenv("REGISTER_COUNT", 1))
SLAVE_ID = int(os.getenv("SLAVE_ID", 1))

# Solid Pod Configuration
SOLID_SERVER = os.getenv("SOLID_SERVER")
RESOURCE_URL = os.getenv("RESOURCE_URL")
COMMANDS_URL = os.getenv("COMMANDS_URL")
OIDC_ISSUER = os.getenv("OIDC_ISSUER")

# CSS Account Credentials
CSS_EMAIL = os.getenv("CSS_EMAIL")
CSS_PASSWORD = os.getenv("CSS_PASSWORD")


