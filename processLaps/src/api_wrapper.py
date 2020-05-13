import abc
import os
import requests
from griiip_exeptions import InterfaceImplementationException


#
# It adds the api key and the base address to every request so you dont need to place it all over the code
# see examples
from interfaces import IDataBaseClient


class ApiWrapper(IDataBaseClient):
    """
    ApiWrapper is client for data base that use api get away
    for crud ops
    """
    @classmethod
    def __init__(cls, **kwargs):
        cls.api_address = kwargs['api_address']
        cls.api_key = kwargs['api_key']

    @classmethod
    def get(cls, url, **kwargs):
        payload = {}
        if kwargs is not None:
            payload = kwargs
        return requests.get(cls.api_address + url, params=payload, headers={'x-api-key': str(cls.api_key)})

    @classmethod
    def put(cls, url, **kwargs):
        body = kwargs
        return requests.put(cls.api_address + url, json=body, headers={'x-api-key': cls.api_key})

    @classmethod
    def post(cls, url, **kwargs):
        body = kwargs
        return requests.post(cls.api_address + url, json=body, headers={'x-api-key': cls.api_key})

    @classmethod
    def delete(cls, url, **kwargs):
        body = kwargs
        return requests.delete(cls.api_address + url, json=body, headers={'x-api-key': cls.api_key})

    @classmethod
    def commit(cls, url, **kwargs):
        pass
