import requests
from solid_client_credentials import SolidClientCredentialsAuth, DpopTokenProvider
#NEED TO FIX THE AUTH VERY SOON
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


def upload_to_solid(oidc_issuer, client_id, client_secret, resource_url, rdf_data):
    token_provider = DpopTokenProvider(
        issuer_url=oidc_issuer, client_id=client_id, client_secret=client_secret
    )
    auth = SolidClientCredentialsAuth(token_provider)
    headers = {"Content-Type": "text/turtle"}

    response = requests.put(resource_url, headers=headers, data=rdf_data, auth=auth)
    

    if response.status_code in [200, 201, 204, 205]:
        return "Data successfully saved in Solid Pod!"
    else:
        return f"Failed to save data ({response.status_code}): {response.text}"

def append_to_solid(oidc_issuer, client_id, client_secret, resource_url, rdf_data):
    token_provider = DpopTokenProvider(
        issuer_url=oidc_issuer, client_id=client_id, client_secret=client_secret
    )
    #NEED TO FIX LATER 
    # auth = SolidClientCredentialsAuth(token_provider)

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

def get_solid_data(oidc_issuer, client_credentials: ClientCredentials, resource_url):
    token_provider = DpopTokenProvider(
        issuer_url=oidc_issuer, client_id=client_credentials.client_id, client_secret=client_credentials.client_secret
    )
    auth = SolidClientCredentialsAuth(token_provider)

    response = requests.get(resource_url, auth=auth)
    print(response.text)
    if response.status_code == 200:
        return response.text
    else:
        return f"Failed to fetch data ({response.status_code}): {response.text}"

