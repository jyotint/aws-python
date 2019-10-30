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
from awsHelper import awsCommonHelper


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
            MESSAGE_ID = "MessageId"
            MD5_MESSAGE_BODY = "MD5OfMessageBody"
            MD5_MESSAGE_ATTRIBUTES = "MD5OfMessageAttributes"
            SEQUENCE_NUMBER = "SequenceNumber"
    class RESPONSE:
        RMESSAGE_ID = "messageId"
        PROVIDER_RESPONSE_DATA = "providerResponseData"
        ADDITIONAL_REQUEST_DATA = "additionalRequestData"
        ARD_MESSAGE_ID = "MessageId"
        ARD_SEQUENCE_NUMBER = "SequenceNumber"
        ARD_MD5_MESSAGE_BODY = "MD5OfMessageBody"
        ARD_MD5_MESSAGE_ATTRIBUTES = "MD5OfMessageAttributes"
    class CONTEXT:
        QUEUE = "queue"
        MESSAGES = "messages"


# Module level variables and code
__sqs = boto3.resource("sqs")           # AWS SQS Resource
logger = logging.getLogger(__name__)    # Logger


# Diagnostics logs
# if("os" in dir()):
#     logger.debug(f"awsSqsHelper::os.getcwd(): '{os.getcwd()}'")
# logger.debug(f"awsSqsHelper::ModuleName:  '{__name__}'")
# logger.debug(f"awsSqsHelper::dir():       {dir()}")


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
            isFifoQueue = awsCommonHelper.getAwsObjectBooleanAttributeValue(queueObj, "FifoQueue", False)
            isContentBasedDeduplication = awsCommonHelper.getAwsObjectBooleanAttributeValue(queueObj, "ContentBasedDeduplication", False)

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
            messageObject[CONSTANTS.SQS.REQUEST.MESSAGE_BODY] = awsCommonHelper.getMessageJsonString(messageBody)
            # Message Attibutes
            if(messageAttributes):
                newNessageAttributes = awsCommonHelper.serializeDictToAttributeList(messageAttributes)
                messageObject[CONSTANTS.SQS.REQUEST.MESSAGE_ATTRIBUTE] = newNessageAttributes
            logger.info(f"awsSqsHelper::sendMessage() >> messageObject: {jsonHelper.convertObjectToFormattedJson(messageObject)}")

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


def __sendMessage(queue, messageObject):
    result = apiMgmt.getDefaultResult()

    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.send_message
        response = queue.send_message(**messageObject)
        logger.info(f"awsSqsHelper::__sendMessage() >> response: {jsonHelper.convertObjectToFormattedJson(response)}")

        responseMetaData = response.get(CONSTANTS.SQS.RESPONSE.RESPONSE_META_DATA)
        result[CONSTANTS.RESPONSE.PROVIDER_RESPONSE_DATA] = responseMetaData

        responeHttpStatusCode = responseMetaData.get(CONSTANTS.SQS.RESPONSE.HTTP_STATUS_CODE)
        if(awsCommonHelper.isResponseHttpStatusCodeSuccess(responeHttpStatusCode)):
            apiMgmt.setResultStatusSuccess(result)
            result[CONSTANTS.RESPONSE.ADDITIONAL_REQUEST_DATA] = __composeSendMessageAdditionalResponseData(response)

    except Exception as ex:
        apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())
        # logger.error(f"awsSqsHelper::__sendMessage() >> Exception has occurred. Error: '{str(ex)}'")

    return result


def __composeSendMessageAdditionalResponseData(response):
    additionalRequestData = {}
    additionalRequestData[CONSTANTS.RESPONSE.ARD_MESSAGE_ID] = response.get(CONSTANTS.SQS.RESPONSE.MESSAGE_ID)
    sequenceNumber = response.get(CONSTANTS.SQS.RESPONSE.SEQUENCE_NUMBER)
    if(sequenceNumber):
        additionalRequestData[CONSTANTS.RESPONSE.ARD_SEQUENCE_NUMBER] = sequenceNumber
    additionalRequestData[CONSTANTS.RESPONSE.ARD_MD5_MESSAGE_BODY] = response.get(CONSTANTS.SQS.RESPONSE.MD5_MESSAGE_BODY)
    additionalRequestData[CONSTANTS.RESPONSE.ARD_MD5_MESSAGE_ATTRIBUTES] = response.get(CONSTANTS.SQS.RESPONSE.MD5_MESSAGE_ATTRIBUTES)
    
    return additionalRequestData
