import os
import boto3
import enum
from .callback_handler import Callback, NotExistingSource
from ..griiip_exeptions import ErrorApiException
from . import logger

codeDeploy = boto3.client('codedeploy')
api = boto3.client('apigateway')
AWS_REGION = os.environ['AWS_REGION']
prefixApiGetAwayUrl = f".execute-api.{AWS_REGION}.amazonaws.com/"


class Messages(enum.Enum):
    resource_not_found = "resource not found"
    succeeded = "Succeeded"
    exist = "resources already exist"
    create = "resources created"
    api_failed = "api call failed"
    create_failed = "failed to create resource"
    check_failed = "resources Check failed"


def lambda_handler(event, context):
    deploymentId, lifecycleEventHookExecutionId = event['deploymentId'], event['lifecycleEventHookExecutionId']
    status = 'Pending'
    # status = 'Succeeded'
    apiResourceCheck()


"""
    finally:
        logger.info(ens)
        codeDeployEns = deploy_(deploymentId=deploymentId,
                                lifecycleEventHookExecutionId=lifecycleEventHookExecutionId,
                                status=status)
        logger.info(f"lambda response: {ens} \n code deployment response : {codeDeployEns}")
        return {'lambdaEns': ens, 'deployEns': codeDeployEns}
"""


def eventSourcingCheck() -> tuple:
    """
    check if the event sources already register if not register them
    :return: (bool, str)
    tuple boole for succeeded or failed and the answer   Succeeded
    """
    try:
        callbackHandler = Callback()

        funcToRegister: [] = os.environ['funcToRegistar'].split(',')
        if len(funcToRegister) <= 0:
            logger.info(f"{Messages.exist.value}")

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
                ens = f"{sourceError}\n {Messages.check_failed.value}"
                logger.debug(ens)
                return False, Messages.check_failed.value

            if res != 200:
                ens = f"callback id {id__}\n didnt register to source event {source}\n eventSourcingCheck failed"
                logger.debug(ens)
                return False, f"{Messages.create_failed} eventSourcing function"

            return True, Messages.succeeded.value

    except ErrorApiException as apiError_critical:
        ens = f"{apiError_critical}\n {Messages.check_failed.value}"
        logger.debug(ens)
        return False, ens

    except Exception as e:
        ens = f"{e}\n {Messages.check_failed.value}"
        logger.debug(ens)
        return False, ens


def apiResourceCheck() -> bool:

    response = api.get_rest_apis(limit=123)
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.debug(f"api getaway return {response['ResponseMetadata']['HTTPStatusCode']}")
        return False, f"api getaway return {response['ResponseMetadata']['HTTPStatusCode']}"

    elif 'items' not in response or len(response['items']) <= 0:
        logger.debug(f"there is no rest apis")
        return False, f"there is no rest apis"

    apiName = os.environ['apiGetAwayName']
    apiIds_ = [a['id'] for a in response['items'] if a['name'] == apiName]

    if len(apiIds_) <= 0:
        res_ = f"{Messages.resource_not_found.value} {apiName}"
        logger.debug(res_)
        return False, res_

    apiId = apiIds_[0]
    response = api.get_resources(
        restApiId=apiId,
        limit=123,
    )

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.debug(f"api getaway return {response['ResponseMetadata']['HTTPStatusCode']}")
        return False, f"{Messages.api_failed.value}: \n {response['ResponseMetadata']['HTTPStatusCode']}"

    elif 'items' not in response or len(response['items']) <= 0:
        logger.debug(f"{Messages.resource_not_found.value}")
        return False, f"{Messages.resource_not_found.value}"

    resourcesToCheck = os.environ['apisRouts'].split(', ')

    existing_resources = [r['pathPart'] for r in response['items'] if 'pathPart' in r and r['pathPart'] in
                          resourcesToCheck]

    if len(existing_resources) == len(resourcesToCheck):
        logger.debug(f'{Messages.exist.value}')
        return True, Messages.exist.value
    parentResourcesPath = os.environ['parentPath']

    try:
        parentId = [r_ for r_ in response['items'] if r_['path'] == parentResourcesPath][0]['id']

    except IndexError as index_e:
        logger.debug(f"{index_e}")
        return False, index_e

    for _r in resourcesToCheck:

        if _r in existing_resources:
            logger.debug(f"{_r} {Messages.exist.value}")
            continue
        response = api.create_resource(
            restApiId=apiId,
            parentId=parentId,
            pathPart=_r
        )

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        logger.debug(f"api getaway return {response['ResponseMetadata']['HTTPStatusCode']}")
        return False, f"{Messages.api_failed.value}: \n {response['ResponseMetadata']['HTTPStatusCode']}"


def sqsResourceCheck():
    pass


def deploy_(deploymentId, lifecycleEventHookExecutionId, status):
    return codeDeploy.put_lifecycle_event_hook_execution_status(deploymentId=deploymentId,
                                                                lifecycleEventHookExecutionId=lifecycleEventHookExecutionId,
                                                                status=status
                                                                )
