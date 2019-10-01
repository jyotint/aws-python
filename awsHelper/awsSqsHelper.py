# Standard library imports
import traceback
import logging
# import os     # Used for Diagnostics
# Third party library imports
import boto3
# Local application imports
from helper import jsonHelper
from helper import apiMgmt


# Module Constants
class CONSTANTS:
    class SQS:
        class REQUEST:
            ID = "Id",
            MESSAGE_BODY = "MessageBody"
            MESSAGE_ATTRIBUTE = "MessageAttributes"
        class RESPONSE:
            RESPONSE_META_DATA = "ResponseMetadata"
            HTTP_STATUS_CODE = "HTTPStatusCode"
            REQUEST_ID = "RequestId"
            MESSAGE_ID = "MessageId"
            MD5_MESSAGE_BODY = "MD5OfMessageBody"
    class RESPONSE:
        RESPONSE_STATUS_CODE = "Response.StatusCode"
        RESPONSE_REQUEST_ID = "Response.RequestId"
        REQUEST_MESSAGE_ID = "Request.MessageId"
        REQUEST_MD5_MESSAGE_BODY = "Request.MD5OfMessageBody"
    class CONTEXT:
        QUEUE = "Queue"
        MESSAGES = "Messages"


# Module level variables and code
__sqs = boto3.resource("sqs")           # AWS SQS Resource
logger = logging.getLogger(__name__)    # Logger


# Diagnostics logs
# if("os" in dir()):
#     logger.debug(f"awsSqsHelper::os.getcwd(): '{os.getcwd()}'")
# logger.debug(f"awsSqsHelper::ModuleName:  '{__name__}'")
# logger.debug(f"awsSqsHelper::dir():       {dir()}")


def __getMessageJsonString(data):
    # logger.debug(f"awsSqsHelper::__getMessageJsonString() >> Message type '{type(data)}'")
    messageString = None
    if(type(data) == type({})):
        messageString = jsonHelper.convertObjectToJson(data)
    elif(type(data) == type("")):
        messageString = data
    else:
        raise Exception(f"Unsupported message type pass, only 'String (containing JSON)' and 'Dictionary' type are supported (passed type: '{type(data)}')")

    return messageString


def __serializeGetAttributeValue(value):
    valueAttr = {}
    valueAttr["DataType"] = "String"
    valueAttr["StringValue"] = value
    return valueAttr

def __deserializeGetAttributeValue(valueAttr):
    return valueAttr["StringValue"]

def serializeDictToAttributeList(datDict):
    newMessageAttributes = {}
    if(datDict):
        for key, value in datDict.items():
            newMessageAttributes[key] = __serializeGetAttributeValue(value)
    return newMessageAttributes

def deserializeAttributeLisToDict(messageAttributes):
    dataDict = {}
    if(messageAttributes):
        for key, value in messageAttributes.items():     
            dataDict[key] = __deserializeGetAttributeValue(value)
    return dataDict


def __sendMessage(queue, messageObject):
    result = apiMgmt.getDefaultResult()

    try:
        response = queue.send_message(**messageObject)
        # logger.debug(f"awsSqsHelper::__sendMessage() >> response: {jsonHelper.convertObjectToFormattedJson(response)}")

        responseMetaData = response.get(CONSTANTS.SQS.RESPONSE.RESPONSE_META_DATA)
        result[CONSTANTS.RESPONSE.REQUEST_MESSAGE_ID] = response.get(CONSTANTS.SQS.RESPONSE.MESSAGE_ID)
        result[CONSTANTS.RESPONSE.REQUEST_MD5_MESSAGE_BODY] = response.get(CONSTANTS.SQS.RESPONSE.MD5_MESSAGE_BODY)
        result[CONSTANTS.RESPONSE.RESPONSE_STATUS_CODE] = responseMetaData.get(CONSTANTS.SQS.RESPONSE.HTTP_STATUS_CODE)
        result[CONSTANTS.RESPONSE.RESPONSE_REQUEST_ID] = responseMetaData.get(CONSTANTS.SQS.RESPONSE.REQUEST_ID)
        apiMgmt.setResultStatusSuccess(result)

    except Exception as ex:
        apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())
        # logger.error(f"awsSqsHelper::__sendMessage() >> Exception has occurred. Error: '{str(ex)}'")

    return result


def getQueue(queueName):
    result = apiMgmt.getDefaultResult()

    try:
        if(queueName == None):
            apiMgmt.setResultFailed(result, exception="Invalid queue Name passed!")
        else:
            queue = __sqs.get_queue_by_name(QueueName=queueName)
            # logger.debug(f"awsSqsHelper::getQueue() >> SQS Queue - {str(queue)}")
            # logger.debug(f"awsSqsHelper::getQueue() >> SQS Queue - Name: '{queueName}', DelaySeconds: '{queue.attributes.get('DelaySeconds')}', Url: '{queue.url}'")
            result[CONSTANTS.CONTEXT.QUEUE] = queue
            apiMgmt.setResultStatusSuccess(result)

    except Exception as ex:
        apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())
        # logger.error(f"awsSqsHelper::getQueue() >> Exception has occurred. Error: '{str(ex)}'")

    return result


def sendMessage(queueName, messageBody, messageAttributes=None, messageId=None):
    result = apiMgmt.getDefaultResult()

    try:
        resultGetQueue = getQueue(queueName)
        if(apiMgmt.isResultFailure(resultGetQueue)):
            result = resultGetQueue
        else:
            messageObject = {}
            if(messageId != None):
                messageObject[CONSTANTS.SQS.REQUEST.ID] = messageId
            if(messageAttributes != None):
                newNessageAttributes = serializeDictToAttributeList(messageAttributes)
                messageObject[CONSTANTS.SQS.REQUEST.MESSAGE_ATTRIBUTE] = newNessageAttributes
            messageObject[CONSTANTS.SQS.REQUEST.MESSAGE_BODY] = __getMessageJsonString(messageBody)
            logger.debug(f"awsSqsHelper::sendMessage() >> messageObject: {jsonHelper.convertObjectToFormattedJson(messageObject)}")

            result = __sendMessage(resultGetQueue.get(CONSTANTS.CONTEXT.QUEUE), messageObject)

    except Exception as ex:
        apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())

    return result


def sendMessages(queueName, messages):
    pass 


def receiveMessages(
    queueName, 
    maxNumberOfMessages=5,
    visibilityTimeoutInSecs=30,
    waitTimeSecondsInSecs=20, 
    attributeNames=['All'],
    messageAttributeNames=['All'],
    messageProcessorFunc=None):
    result = apiMgmt.getDefaultResult()

    try:
        resultGetQueue = getQueue(queueName)
        if(apiMgmt.isResultFailure(resultGetQueue)):
            result = resultGetQueue
        else:
            logger.info(f"awsSqsHelper::receiveMessages() >> parameters:")
            logger.info(f"  MaxNumberOfMessages: {maxNumberOfMessages}, VisibilityTimeout: {visibilityTimeoutInSecs} seconds, WaitTimeSeconds: {waitTimeSecondsInSecs} seconds")
            logger.info(f"  QueueName: '{queueName}', MessageAttributeNames: {messageAttributeNames}, AttributeNames: {attributeNames}")
            
            # result = __receiveMessages(resultGetQueue.get(CONSTANTS.CONTEXT.QUEUE))
            queue = resultGetQueue.get(CONSTANTS.CONTEXT.QUEUE)
            messages = queue.receive_messages(
                AttributeNames=attributeNames,
                MessageAttributeNames=messageAttributeNames,
                MaxNumberOfMessages=maxNumberOfMessages,
                VisibilityTimeout=visibilityTimeoutInSecs,
                WaitTimeSeconds=waitTimeSecondsInSecs
            )

            result[CONSTANTS.CONTEXT.MESSAGES] = messages
            apiMgmt.setResultStatusSuccess(result)

    except Exception as ex:
        apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())

    return result


def deleteMessage(message):
    result = apiMgmt.getDefaultResult()

    try:
        result = message.delete()
        apiMgmt.setResultStatusSuccess(result)

    except Exception as ex:
        apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())

    return result
