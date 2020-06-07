import os
from logging import error, debug, warning, critical, info
from ..db_wrapper import DbApiWrapper
from ..griiip_exeptions import ErrorApiException
from ..griiip_const import net


class NotExistingSource(Exception):
    def __init__(self, source):
        Exception.__init__(self, f"there is no source {source}")


class Callback:
    _apiKey, _apiUrl = os.environ['griiip_api_key'], os.environ['griiip_api_url']
    api: DbApiWrapper = DbApiWrapper(api_address=_apiUrl, api_key=_apiKey)

    def __init__(self):
        pass

    def getApiUrl(self):
        return self._apiUrl

    def getApiKey(self):
        return self._apiKey

    def getExistingCallBacks(self) -> []:

        try:
            httpsRes_ = self.api.get(f"{net.CALLBACK}")
            if httpsRes_.status_code != 200:
                raise ErrorApiException(f'get: {net.CALLBACK}',
                                        httpsRes_.status_code,
                                        httpsRes_.text
                                        )

            notificationList: [] = httpsRes_.json()
            return [f['url'] for f in notificationList]

        except ErrorApiException as api_e:
            critical(api_e)
            raise api_e

        except Exception as e:
            warning(e)
            raise e

    def putCallback(self, url: str, headers: {} = None):
        headers_: {} = {
            "X-Api-Key": self._apiKey
        }
        if headers is not None:
            headers_ = {**headers_, **headers}
        bodyToSend: {} = {"url": f"{self._apiUrl}/{url}", "headers": headers_}

        httpsRes_ = self.api.put(f"{net.CALLBACK}", json={**bodyToSend})
        if httpsRes_.status_code != 200:
            raise ErrorApiException(f"{net.CALLBACK}",
                                    httpsRes_.status_code,
                                    httpsRes_.text
                                    )

        return httpsRes_.json()['id']

    def subscribeToEvent(self, sourceName: str, callBackId: str):

        try:
            httpsRes_ = self.api.get(f"{net.CALLBACK_SOURCE}")
            if httpsRes_.status_code != 200:
                raise ErrorApiException(net.CALLBACK_SOURCE,
                                        httpsRes_.status_code,
                                        httpsRes_.text
                                        )

            sources: [] = httpsRes_.json()
            source = [s for s in sources if s['name'] == sourceName][0]
            if source is None:
                raise NotExistingSource(source['_id'])

            info(f"going to register to event : {source['name']}")
            url = f"{net.CALLBACK_SOURCE}/{source['_id']}/callback/{callBackId}"

            httpsRes_ = self.api.put(url)
            if httpsRes_.status_code != 200:
                raise ErrorApiException(url,
                                        httpsRes_.status_code,
                                        httpsRes_.text
                                        )

            info(f"{url}: \n {httpsRes_}")

            return httpsRes_.status_code
        except ErrorApiException as api_e:
            critical(api_e)
            raise api_e

        except Exception as e:
            warning(e)
            raise e
