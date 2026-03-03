import requests
from handlers.solid_handler import *
from config import (
    SOLID_SERVER, RESOURCE_URL, OIDC_ISSUER, CSS_EMAIL, CSS_PASSWORD, CLIENT_ID, CLIENT_SECRET
)

class CssAccount:
    def __init__(self, css_base_url, email, password):
        self.css_base_url = css_base_url
        self.email = email
        self.password = password


class ClientCredentials:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret


def get_client_credentials(account: CssAccount) -> ClientCredentials:
    credentials_endpoint = f"{account.css_base_url}/idp/credentials/"

    response = requests.post(
        credentials_endpoint,
        json={"name": "my-token", "email": account.email, "password": account.password},
        timeout=5000, verify=False
    )

    if not response.ok:
        raise Exception(
            f"Could not create client credentials ({response.status_code}): {response.text}"
        )

    data = response.json()
    return ClientCredentials(client_id=data["id"], client_secret=data["secret"])

class SolidServer:
    def __init__(self):
        self.solid_server = SOLID_SERVER
        self.resource_url = RESOURCE_URL
        self.oidc_issuer = OIDC_ISSUER
        self.css_account = CssAccount(SOLID_SERVER,CSS_EMAIL,CSS_PASSWORD)
        self.client_credentials = get_client_credentials(self.css_account)
        self.token_provider = DpopTokenProvider(
            issuer_url=self.oidc_issuer, client_id=self.client_credentials.client_id, client_secret=self.client_credentials.client_secret
        )
        auth = auth = SolidClientCredentialsAuth(self.token_provider)



    def upload(self, resource_url, rdf_data):
        headers = {"Content-Type": "text/turtle"}

        response = requests.put(resource_url, headers=headers, data=rdf_data, auth=self.auth)
        

        if response.status_code in [200, 201, 204, 205]:
            return "Data successfully saved in Solid Pod!"
        else:
            return f"Failed to save data ({response.status_code}): {response.text}"

    def append(self,resource_url, rdf_data):
        # First, retrieve the existing data
        get_response = requests.get(resource_url, headers={"Accept": "text/turtle"},verify=False)

        if get_response.status_code == 200:
            existing_data = get_response.text
        else:
            existing_data = ""  # If the resource doesn't exist, create new data

        # Append new RDF data
        updated_data = existing_data + "\n" + rdf_data

        # Send a PUT request with the updated RDF data
        headers = {"Content-Type": "text/turtle"}
        put_response = requests.put(resource_url, headers=headers, data=updated_data,verify=False)

        if put_response.status_code in [200, 201, 204, 205]:
            print("successful upload")
            return "Data successfully updated in Solid Pod!"
        else:
            return f"Failed to update data ({put_response.status_code}): {put_response.text}"

    def get_solid_data(self, resource_url):
        response = requests.get(resource_url, auth=self.auth)
        print(response.text)
        if response.status_code == 200:
            return response.text
        else:
            return f"Failed to fetch data ({response.status_code}): {response.text}"
    
