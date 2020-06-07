import os
from .callback_handler import Callback, NotExistingSource
from ..griiip_exeptions import ErrorApiException
from . import logger


def lambda_handler(event, context):
    try:
        ens: [] = []
        callbackHandler = Callback()

        funcToRegister: [] = os.environ['funcToRegistar'].split(',')
        if len(funcToRegister) <= 0:
            ens.append("all callbacks are register all ready")

        callBacksIds_: [] = []
        for f_ in funcToRegister:
            url = f_
            id_ = callbackHandler.putCallback(url)
            callBacksIds_.append(id_)

        for id__ in callBacksIds_:
            source = os.environ['notificationSorceName']
            try:
                res = callbackHandler.subscribeToEvent(sourceName=source,
                                                       callBackId=id__)
            except NotExistingSource as sourceError:
                logger.error(sourceError)
                ens.append(sourceError)
                pass

            if res != 200:
                logger.error(f"callback id {id__}\n didnt register to source event {source}")

            ens.append(res)

    except ErrorApiException as apiError_critical:
        ens.append(apiError_critical)
        return

    except Exception as e:
        ens.append(e)
        pass

    finally:
        logger.info(ens)
        return ens
