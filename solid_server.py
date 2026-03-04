import requests
from urllib.parse import urljoin
from solid_client_credentials import SolidClientCredentialsAuth, DpopTokenProvider
from rdflib import Graph, Namespace, URIRef, Literal
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
        auth = SolidClientCredentialsAuth(self.token_provider)



    def upload(self, resource_url, rdf_data):
        headers = {"Content-Type": "text/turtle"}

        response = requests.put(resource_url, headers=headers, data=rdf_data, auth=self.auth)
        

        if response.status_code in [200, 201, 204, 205]:
            return "Data successfully saved in Solid Pod!"
        else:
            return f"Failed to save data ({response.status_code}): {response.text}"

    def append(self, rdf_graph):
        try:
            subject_uri = str(next(rdf_graph.subjects()))
            parts = subject_uri.split('/')
            
            if 'slave' in parts:
                slave_idx = parts.index('slave')
                base_slave_path = "/".join(parts[:slave_idx+2])
                target_url = f"{base_slave_path}/data.ttl"
            else:
                target_url = f"{subject_uri.rsplit('/', 1)[0]}/data.ttl"
            
            prefixes = "\n".join([f"PREFIX {p}: <{n}>" for p, n in rdf_graph.namespaces()])
            triples = " .\n".join([f"{s.n3()} {p.n3()} {o.n3()}" for s, p, o in rdf_graph])
            sparql_query = f"{prefixes}\nINSERT DATA {{ {triples} }}"
            
            headers = {
                "Content-Type": "application/sparql-update",
                "Link": '<http://www.w3.org/ns/ldp#Resource>; rel="type"'
            }
            
            response = requests.patch(
                target_url,
                headers=headers,
                data=sparql_query,
                verify=False
            )

            if response.status_code in [200, 201, 204, 205]:
                print(f"Success: {target_url}")
            else:
                print(f"Error {response.status_code}: {response.text}")
                
            return response.status_code

        except StopIteration:
            return None
        except Exception as e:
            print(f"Error: {e}")
            return None

    def get_solid_data(self, resource_url):
        response = requests.get(resource_url, auth=self.auth)
        print(response.text)
        if response.status_code == 200:
            print(response.text)
            return response.text
        else:
            return f"Failed to fetch data ({response.status_code}): {response.text}"
    
    def delete_resource(self, url):
        """Deletes a specific resource or container from the Pod."""
        response = requests.delete(
            url,
            verify=False
        )
        if response.status_code in [200, 204]:
            print(f"Successfully deleted: {url}")
        else:
            print(f"Failed to delete {url}: {response.status_code} - {response.text}")

    def delete_container(self, container_url):
        if not container_url.endswith('/'):
            container_url += '/'

        # Removed auth=self.auth
        response = requests.get(container_url, verify=False, 
                                headers={"Accept": "text/turtle"})
        
        if response.status_code == 200:
            g = Graph()
            g.parse(data=response.text, format="turtle", publicID=container_url)
            LDP = Namespace("http://www.w3.org/ns/ldp#")
            
            for _, _, child_url in g.triples((None, LDP.contains, None)):
                full_child_url = urljoin(container_url, str(child_url))
                if full_child_url.endswith('/'):
                    self.delete_container(full_child_url)
                else:
                    # Removed auth=self.auth
                    requests.delete(full_child_url, verify=False)
            
        # Removed auth=self.auth
        return requests.delete(container_url, verify=False).status_code
    
