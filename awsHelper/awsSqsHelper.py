# Standard library imports
import traceback
# import os     # Used for Diagnostics
# Third party library imports
import boto3
# Local application importsimport os
from helper import jsonHelper


# Module Constants
class CONTANTS:
    class SQS:
        class REQUEST:
            ID = "Id",
            MESSAGE_BODY = "MessageBody"
        class RESPONSE:
            RESPONSE_META_DATA = "ResponseMetadata"
            HTTP_STATUS_CODE = "HTTPStatusCode"
            REQUEST_ID = "RequestId"
            MESSAGE_ID = "MessageId"
            MD5_MESSAGE_BODY = "MD5OfMessageBody"
    class RESPONSE:
        STATUS = "Status"
        EXCEPTION = "Exception"
        STACK_TRACE = "StackTrace"
        RESPONSE_STATUS_CODE = "Response.StatusCode"
        RESPONSE_REQUEST_ID = "Response.RequestId"
        REQUEST_MESSAGE_ID = "Request.MessageId"
        REQUEST_MD5_MESSAGE_BODY = "Request.MD5OfMessageBody"
    class CONTEXT:
        QUEUE_OBJECT = "QueueObject"


# AWS SQS Resource
__sqs = boto3.resource("sqs")


# Diagnostics logs
# print(f"awsSqsHelper::os.getcwd(): '{os.getcwd()}'")
# print(f"awsSqsHelper::ModuleName:  '{__name__}'")
# print(f"awsSqsHelper::dir():       {dir()}")


def __getDefaultResult():
    return {
        CONTANTS.RESPONSE.STATUS: False
    }


def __getMessageString(data):
    # print(f"awsSqsHelper::__getMessageString() >> Message type '{type(data)}'")
    messageString = None
    if(type(data) == type({})):
        messageString = jsonHelper.convertToString(data)
    elif(type(data) == type("")):
        messageString = data
    else:
        raise Exception(f"Unsupported message type pass, only 'String' and 'Dictionary' type are supported (passed type: '{type(data)}')")

    return messageString


def __sendMessage(queue, messageObject):
    result = __getDefaultResult()

    try:
        response = queue.send_message(**messageObject)
        # print("awsSqsHelper::__sendMessage() >> response: ", jsonHelper.getFormattedJson(response))

        result[CONTANTS.RESPONSE.STATUS] = True
        responseMetaData = response.get(CONTANTS.SQS.RESPONSE.RESPONSE_META_DATA)
        result[CONTANTS.RESPONSE.REQUEST_MESSAGE_ID] = response.get(CONTANTS.SQS.RESPONSE.MESSAGE_ID)
        result[CONTANTS.RESPONSE.REQUEST_MD5_MESSAGE_BODY] = response.get(CONTANTS.SQS.RESPONSE.MD5_MESSAGE_BODY)
        result[CONTANTS.RESPONSE.RESPONSE_STATUS_CODE] = responseMetaData.get(CONTANTS.SQS.RESPONSE.HTTP_STATUS_CODE)
        result[CONTANTS.RESPONSE.RESPONSE_REQUEST_ID] = responseMetaData.get(CONTANTS.SQS.RESPONSE.REQUEST_ID)

    except Exception as ex:
        result[CONTANTS.RESPONSE.EXCEPTION] = ex.__str__()
        result[CONTANTS.RESPONSE.STACK_TRACE] = traceback.format_exc()
        # print("awsSqsHelper::__sendMessage() >> Exception has occurred. Error: '{ex.__str__()}'")

    return result


def getQueue(queueName):
    result = __getDefaultResult()

    try:
        if(queueName == None):
            raise Exception(f"Invalid queue Name passed!")

        queue = __sqs.get_queue_by_name(QueueName=queueName)
        # print(f"awsSqsHelper::getQueue() >> SQS Queue - Name: '{queueName}', DelaySeconds: '{queue.attributes.get('DelaySeconds')}', Url: '{queue.url}'")
        result[CONTANTS.CONTEXT.QUEUE_OBJECT] = queue
        result[CONTANTS.RESPONSE.STATUS] = True

    except Exception as ex:
        result[CONTANTS.RESPONSE.EXCEPTION] = ex.__str__()
        result[CONTANTS.RESPONSE.STACK_TRACE] = traceback.format_exc()
        # print(f"awsSqsHelper::getQueue() >> Exception has occurred. Error: '{ex.__str__()}'")

    return result


def sendMessage(queueName, message, messageId=None):
    result = __getDefaultResult()

    try:
        resultGetQueue = getQueue(queueName)
        if(resultGetQueue.get(CONTANTS.RESPONSE.STATUS) == False):
            result = resultGetQueue
        else:
            messageObject = {}
            if(messageId != None):
                messageObject[CONTANTS.SQS.REQUEST.ID] = messageId 
            messageObject[CONTANTS.SQS.REQUEST.MESSAGE_BODY] = __getMessageString(message)
            print("awsSqsHelper::sendMessage() >> messageObject: ", messageObject)

            result = __sendMessage(resultGetQueue.get(CONTANTS.CONTEXT.QUEUE_OBJECT), messageObject)

    except Exception as ex:
        result[CONTANTS.RESPONSE.EXCEPTION] = ex.__str__()
        result[CONTANTS.RESPONSE.STACK_TRACE] = traceback.format_exc()

    return result


def sendMessages(queueName, messages):
    pass
