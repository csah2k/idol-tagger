
import requests
import concurrent.futures
from retrying import retry

class Client(object):
    def __init__(self, entrypoint, username=None, password=None):
        self.entrypoint = entrypoint
        self.client = requests.Session()

        api_token = self.get_api_token(username, password)
        self.auth_headers = {"Authorization": "Token {}".format(api_token)}

    def get_api_token(self, username, password):
        url = f"{self.entrypoint}/v1/auth-token"
        login = {"username": username, "password": password}
        response = self.client.post(url, json=login)
        return response.json()["token"]

    def fetch_projects(self):
        url = f"{self.entrypoint}/v1/projects"
        response = self.client.get(url, headers=self.auth_headers)
        return response