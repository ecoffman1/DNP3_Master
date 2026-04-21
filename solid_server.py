import asyncio
import json
import ssl
import threading

import requests
import websockets
from functools import wraps
from urllib.parse import urljoin
from solid_client_credentials import SolidClientCredentialsAuth, DpopTokenProvider
from rdflib import Graph, Namespace, URIRef, Literal

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# solid_client_credentials calls requests without verify=False, so patch both
_original_requests_get = requests.get
_original_requests_post = requests.post

@wraps(_original_requests_get)
def _insecure_get(*args, **kwargs):
    kwargs.setdefault("verify", False)
    return _original_requests_get(*args, **kwargs)

@wraps(_original_requests_post)
def _insecure_post(*args, **kwargs):
    kwargs.setdefault("verify", False)
    return _original_requests_post(*args, **kwargs)

requests.get = _insecure_get
requests.post = _insecure_post

from config import (
    SOLID_SERVER, RESOURCE_URL, OIDC_ISSUER, CSS_EMAIL, CSS_PASSWORD, PORTAL_WEB_ID
)
from load_devices import SOLID_DEVICES

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
        self.solid_server = SOLID_SERVER.rstrip("/")
        self.resource_url = RESOURCE_URL
        self._device_auth = {}  # device_key -> SolidClientCredentialsAuth



    def register_account(self, email: str, password: str, pod_name: str) -> dict:
        """
        Registers a new account on the CSS instance via /idp/register/.
        Returns a dict with 'webId' and 'podBaseUrl' on success.
        """
        base = self.solid_server.rstrip("/")
        response = requests.post(
            f"{base}/idp/register/",
            json={
                "email": email,
                "password": password,
                "confirmPassword": password,
                "podName": pod_name,
                "createWebId": "on",
                "createPod": "on",
                "register": "on",
            },
            timeout=10,
            verify=False,
        )

        if response.status_code == 400 and "already" in response.text.lower():
            return None  # account already exists

        if not response.ok:
            raise Exception(
                f"Registration failed ({response.status_code}): {response.text}"
            )

        data = response.json()
        return {"webId": data["webId"], "podBaseUrl": data["podBaseUrl"]}

    def _build_auth(self, email: str, password: str) -> SolidClientCredentialsAuth:
        account = CssAccount(self.solid_server, email, password)
        creds = get_client_credentials(account)
        token_provider = DpopTokenProvider(
            issuer_url=self.solid_server,
            client_id=creds.client_id,
            client_secret=creds.client_secret,
        )
        return SolidClientCredentialsAuth(token_provider)

    def provision_devices(self):
        for device_key, info in SOLID_DEVICES.items():
            try:
                result = self.register_account(
                    email=info["email"],
                    password=info["password"],
                    pod_name=device_key,
                )
                if result is None:
                    print(f"[{device_key}] Account already exists")
                else:
                    print(f"[{device_key}] Account created — WebID: {result['webId']}")
            except Exception as e:
                print(f"[{device_key}] Registration error: {e}")

            try:
                auth = self._build_auth(info["email"], info["password"])
                self._device_auth[device_key] = auth
                print(f"[{device_key}] Auth ready")
            except Exception as e:
                print(f"[{device_key}] Auth error: {e}")
                continue

            try:
                self._set_data_acl(device_key, auth)
                print(f"[{device_key}] Data ACL set (utility read-only)")
            except Exception as e:
                print(f"[{device_key}] Data ACL error: {e}")

            try:
                self._set_commands_acl(device_key, auth)
                print(f"[{device_key}] Commands ACL set")
            except Exception as e:
                print(f"[{device_key}] Commands ACL error: {e}")

    def _set_data_acl(self, device_key: str, auth: SolidClientCredentialsAuth):
        """PUT a WAC ACL on the dnp3/ (write_dir) container.

        Device pod owner: Read, Write, Control.
        Utility account:  Read only.
        No public access.
        """
        if not PORTAL_WEB_ID:
            raise Exception("PORTAL_WEB_ID must be set to configure data ACL")

        info = SOLID_DEVICES[device_key]
        container = info["write_dir"].rstrip("/") + "/"
        acl_url = container + ".acl"
        web_id = info["webId"]

        acl_body = f"""@prefix acl: <http://www.w3.org/ns/auth/acl#> .

<#owner>
    a acl:Authorization ;
    acl:agent <{web_id}> ;
    acl:accessTo <{container}> ;
    acl:default <{container}> ;
    acl:mode acl:Read, acl:Write, acl:Control .

<#utility>
    a acl:Authorization ;
    acl:agent <{PORTAL_WEB_ID}> ;
    acl:accessTo <{container}> ;
    acl:default <{container}> ;
    acl:mode acl:Read .
"""
        response = requests.put(
            acl_url,
            headers={"Content-Type": "text/turtle"},
            data=acl_body,
            auth=auth,
            verify=False,
            timeout=10,
        )
        if not response.ok:
            raise Exception(f"Data ACL PUT failed ({response.status_code}): {response.text}")

    def _set_commands_acl(self, device_key: str, auth: SolidClientCredentialsAuth):
        """PUT a WAC ACL on the dnp3_commands/ container.

        Device pod owner: Read, Write, Control.
        Utility account:  Read, Write (so the dashboard can post commands).
        No public access.
        """
        if not PORTAL_WEB_ID:
            raise Exception("PORTAL_WEB_ID must be set to configure commands ACL")

        info = SOLID_DEVICES[device_key]
        base = self.solid_server.rstrip("/")
        container = f"{base}/{device_key}/dnp3_commands/"
        acl_url = container + ".acl"
        web_id = info["webId"]

        acl_body = f"""@prefix acl: <http://www.w3.org/ns/auth/acl#> .

<#owner>
    a acl:Authorization ;
    acl:agent <{web_id}> ;
    acl:accessTo <{container}> ;
    acl:default <{container}> ;
    acl:mode acl:Read, acl:Write, acl:Control .

<#utility>
    a acl:Authorization ;
    acl:agent <{PORTAL_WEB_ID}> ;
    acl:accessTo <{container}> ;
    acl:default <{container}> ;
    acl:mode acl:Read, acl:Write .
"""
        response = requests.put(
            acl_url,
            headers={"Content-Type": "text/turtle"},
            data=acl_body,
            auth=auth,
            verify=False,
            timeout=10,
        )
        if not response.ok:
            raise Exception(f"Commands ACL PUT failed ({response.status_code}): {response.text}")

    def upload(self, resource_url, rdf_data):
        headers = {"Content-Type": "text/turtle"}

        response = requests.put(resource_url, headers=headers, data=rdf_data, auth=self.auth)
        

        if response.status_code in [200, 201, 204, 205]:
            return "Data successfully saved in Solid Pod!"
        else:
            return f"Failed to save data ({response.status_code}): {response.text}"

    def _refresh_auth(self, device_key: str):
        """Rebuild and cache a fresh auth object for the given device."""
        info = SOLID_DEVICES[device_key]
        self._device_auth[device_key] = self._build_auth(info["email"], info["password"])

    def append(self, rdf_graph, device_key):
        try:
            write_dir = SOLID_DEVICES[device_key]["write_dir"].rstrip("/")
            target_url = f"{write_dir}/data.ttl"

            # Prepare SPARQL Update
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
                auth=self._device_auth.get(device_key),
                verify=False,
                timeout=30,
            )

            if response.status_code == 401:
                # Token expired — refresh auth and retry once
                self._refresh_auth(device_key)
                response = requests.patch(
                    target_url,
                    headers=headers,
                    data=sparql_query,
                    auth=self._device_auth.get(device_key),
                    verify=False,
                    timeout=30,
                )

            if response.status_code not in [200, 201, 204, 205]:
                print(f"Error {response.status_code}: {response.text}")

            return response.status_code

        except StopIteration:
            return None
        except Exception as e:
            print(f"Error in append: {e}")
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
    
    def read_data(self, slave_id):
        target_url = f"{self.resource_url}/slave/{slave_id}/data.ttl"
        
        try:
            response = requests.get(target_url, verify=False)
            
            if response.status_code == 200:
                g = Graph()
                g.parse(data=response.text, format="turtle")
                return g
            else:
                print(f"Read Error {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Error: {e}")
            return None
        
    def print_readings(self,rdf_graph):
        if not rdf_graph:
            return

        DNP3 = Namespace("https://ec2-34-201-119-230.compute-1.amazonaws.com/char/dnp3/#")

        for subject in rdf_graph.subjects(unique=True):
            reg = rdf_graph.value(subject, DNP3.register)
            val = rdf_graph.value(subject, DNP3.value)
            time = rdf_graph.value(subject, DNP3.accessed)
            group = rdf_graph.value(subject, DNP3.func_code)

            if all(v is not None for v in [reg, val, time, group]):
                print(f"[{time}] Group {group} | Reg {reg} | Val: {val}")

    # ------------------------------------------------------------------
    # Command listening (Solid → DNP3)
    # ------------------------------------------------------------------

    def get_websocket_url(self, device_key: str, commands_url: str) -> str:
        """Subscribe to Solid WebSocket notifications for a commands resource."""
        auth = self._device_auth.get(device_key)
        print(f"[{device_key}] WebSocket subscription auth={auth}")
        payload = {
            "@context": ["https://www.w3.org/ns/solid/notification/v1"],
            "type": "http://www.w3.org/ns/solid/notifications#WebSocketChannel2023",
            "topic": commands_url,
        }
        url = self.solid_server + "/.notifications/WebSocketChannel2023/"
        response = requests.post(
            url,
            headers={"Content-Type": "application/ld+json"},
            json=payload,
            auth=auth,
            verify=False,
            timeout=10,
        )
        if response.status_code == 200:
            ws_url = response.json().get("receiveFrom")
            print(f"[{device_key}] WebSocket URL: {ws_url}")
            return ws_url
        else:
            raise Exception(
                f"[{device_key}] WebSocket subscription failed ({response.status_code}): {response.text}"
            )

    def get_command(self, commands_url: str, device_key: str) -> dict:
        """Fetch a command from a Solid resource.

        Accepts two formats:
          - Plain text "1" (sim on) or "0" (sim off) — written by the dashboard
          - JSON {"index": 0, "turn_on": true}
        """
        auth = self._device_auth.get(device_key)
        response = requests.get(commands_url, auth=auth, verify=False, timeout=10)
        if response.status_code != 200:
            raise Exception(
                f"Failed to fetch command ({response.status_code}): {response.text}"
            )
        text = response.text.strip()
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
        # Plain-text convention: "1" = on, "0" = off
        return {"turn_on": text == "1"}

    async def _ws_listener(self, websocket_url: str, callback):
        """Async loop that calls callback(object_url) on every WebSocket message."""
        ssl_context = ssl._create_unverified_context()
        async with websockets.connect(websocket_url, ssl=ssl_context) as websocket:
            print(f"Connected to WebSocket: {websocket_url}")
            while True:
                message = await websocket.recv()
                data = json.loads(message)
                object_url = data.get("object")
                if object_url:
                    callback(object_url)

    def start_websocket_listener(self, device_key: str, commands_url: str, callback):
        """Start a background thread that listens for command notifications on Solid."""
        def run():
            try:
                ws_url = self.get_websocket_url(device_key, commands_url)
                asyncio.run(self._ws_listener(ws_url, callback))
            except Exception as e:
                print(f"[{device_key}] WebSocket listener error: {e}")

        t = threading.Thread(target=run, daemon=True, name=f"ws-{device_key}")
        t.start()
        return t