import abc
import os
import requests
from griiip_exeptions import InterfaceImplementationException


#
# It adds the api key and the base address to every request so you dont need to place it all over the code
# see examples
from interfaces import IApiWrapper


class ApiWrapper(IApiWrapper):

    @classmethod
    def __init__(cls, **kwargs):
        cls.api_address = kwargs['api_address']
        cls.api_key = kwargs['api_key']

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
