import os
import requests


#
# Shlomi TODO: Please use this class in order to do the connections to the API
# It adds the api key and the base address to every request so you dont need to place it all over the code
# see examples


class ApiWrapper:
    api_address = os.environ['griiip_api_url']
    api_key = os.environ['griiip_api_key']

    @classmethod
    def get(cls, url, **kwargs):
        payload = {}
        if 'params' in kwargs:
            payload = kwargs['params']
        return requests.get(cls.api_address + url, params=payload, headers={'x-api-key': str(cls.api_key)})

    @classmethod
    def put(cls, url, **kwargs):
        body = kwargs['json']
        return requests.put(cls.api_address + url, json=body, headers={'x-api-key': cls.api_key})

    @classmethod
    def post(cls, url, **kwargs):
        body = kwargs['json']
        return requests.post(cls.api_address + url, json=body, headers={'x-api-key': cls.api_key})

