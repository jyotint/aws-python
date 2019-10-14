# Standard library imports
import traceback
import logging
import uuid
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
            MESSAGE_GROUP_ID = "MessageGroupId"
            MESSAGE_DEDUPLICATION_ID = "MessageDeduplicationId"
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


def getAwsObjectBooleanAttributeValue(awsObj, attr, defaultValue=False):
    value = awsObj.attributes.get(attr, defaultValue)
    if(type(value) == type('')):
        if(value == 'true'):
            value = True
        elif(value == 'false'):
            value = False
    return value


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
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.ServiceResource.get_queue_by_name
            queue = __sqs.get_queue_by_name(QueueName=queueName)
            result[CONSTANTS.CONTEXT.QUEUE] = queue
            apiMgmt.setResultStatusSuccess(result)

            logger.info(f"awsSqsHelper::getQueue() >> SQS Queue - Name: '{queueName}'\
, DelaySeconds: '{queue.attributes.get('DelaySeconds')}'\
, FifoQueue: '{queue.attributes.get('FifoQueue')}'\
, ContentBasedDeduplication: '{queue.attributes.get('ContentBasedDeduplication')}'\
, Url: '{queue.url}'")

    except Exception as ex:
        apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())
        # logger.error(f"awsSqsHelper::getQueue() >> Exception has occurred. Error: '{str(ex)}'")

    return result


def sendMessage(queueName, messageBody, messageAttributes=None, messageId=None, messageGroupId=None, messageDeduplicationId=None):
    result = apiMgmt.getDefaultResult()

    try:
        resultGetQueue = getQueue(queueName)
        if(apiMgmt.isResultFailure(resultGetQueue)):
            result = resultGetQueue
        else:
            queueObj = resultGetQueue.get(CONSTANTS.CONTEXT.QUEUE)
            isFifoQueue = getAwsObjectBooleanAttributeValue(queueObj, "FifoQueue", False)
            isContentBasedDeduplication = getAwsObjectBooleanAttributeValue(queueObj, "ContentBasedDeduplication", False)

            messageObject = {}
            # Message ID
            if(messageId):
                messageObject[CONSTANTS.SQS.REQUEST.ID] = messageId
            # Message Group ID
            #       The tag that specifies that a message belongs to a specific message group. Messages that belong to the same message 
            #       group are processed in a FIFO manner (however, messages in different message groups might be processed out of order)
            if(isFifoQueue == True and messageGroupId == None):
                messageObject[CONSTANTS.SQS.REQUEST.MESSAGE_GROUP_ID] = str(uuid.uuid4())
            elif(isFifoQueue == True and messageGroupId):
                messageObject[CONSTANTS.SQS.REQUEST.MESSAGE_GROUP_ID] = messageGroupId
            # Message Deduplication ID
            #       The token used for deduplication of sent messages, Use for deduplicating messages with Deduplication Interval
            #       If ContentBasedDeduplication is enabled then this parameter is optional, if provided it will be used first
            if(isFifoQueue == True and messageDeduplicationId):
                messageObject[CONSTANTS.SQS.REQUEST.MESSAGE_DEDUPLICATION_ID] = messageDeduplicationId
            elif(isFifoQueue == True and messageDeduplicationId == None and isContentBasedDeduplication == False):
                messageObject[CONSTANTS.SQS.REQUEST.MESSAGE_DEDUPLICATION_ID] = str(uuid.uuid4())
            # Message Body
            messageObject[CONSTANTS.SQS.REQUEST.MESSAGE_BODY] = __getMessageJsonString(messageBody)
            # Message Attibutes
            if(messageAttributes):
                newNessageAttributes = serializeDictToAttributeList(messageAttributes)
                messageObject[CONSTANTS.SQS.REQUEST.MESSAGE_ATTRIBUTE] = newNessageAttributes
            logger.info(f"awsSqsHelper::sendMessage() >> messageObject: {jsonHelper.convertObjectToFormattedJson(messageObject)}")

            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.send_message
            result = __sendMessage(queueObj, messageObject)

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
            
            queue = resultGetQueue.get(CONSTANTS.CONTEXT.QUEUE)
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.receive_messages
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
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Message.delete
        result = message.delete()
        apiMgmt.setResultStatusSuccess(result)

    except Exception as ex:
        apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())

    return result
