import os
import urllib3
from dotenv import load_dotenv
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()
from handlers.solid_handler import *

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

account = CssAccount(css_base_url=SOLID_SERVER, email=CSS_EMAIL, password=CSS_PASSWORD)

try:
    client_credentials = get_client_credentials(account)
    CLIENT_ID = client_credentials.client_id
    CLIENT_SECRET = client_credentials.client_secret
    print(f"Client ID: {CLIENT_ID}")
except Exception as e:
    print(f"Error fetching client credentials: {e}")
    
