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
    class SNS:
        class REQUEST:
            ID = "Id",
            TARGET_ARN = "TargetArn"
            MESSAGE_BODY = "Message"
            MESSAGE_ATTRIBUTE = "MessageAttributes"
            SUBJECT = "Subject"
        class RESPONSE:
            RESPONSE_META_DATA = "ResponseMetadata"
            HTTP_STATUS_CODE = "HTTPStatusCode"
            TOPICS = "Topics"
            TOPIC_ARN = "TopicArn"
            MESSAGE_ID = "MessageId"
    class RESPONSE:
        MESSAGE_ID = "messageId"
        PROVIDER_RESPONSE_DATA = "providerResponseData"
    class CONTEXT:
        TOPIC_ARN = "topicArn"


# Module level variables and code
__snsClient = boto3.client("sns")       # AWS SNS Client
logger = logging.getLogger(__name__)    # Logger


# Diagnostics logs
# if("os" in dir()):
#     logger.debug(f"awsSnsHelper::os.getcwd(): '{os.getcwd()}'")
# logger.debug(f"awsSnsHelper::ModuleName:  '{__name__}'")
# logger.debug(f"awsSnsHelper::dir():       {dir()}")




def getTopic(topicName):
    result = apiMgmt.getDefaultResult()

    try:
        if(topicName == None):
            apiMgmt.setResultFailed(result, exception="Invalid Topic Name passed!")
        else:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.list_topics
            response = __snsClient.list_topics()
            responseMetaData = response.get(CONSTANTS.SNS.RESPONSE.RESPONSE_META_DATA)
            result[CONSTANTS.RESPONSE.PROVIDER_RESPONSE_DATA] = responseMetaData

            responeHttpStatusCode = responseMetaData.get(CONSTANTS.SNS.RESPONSE.HTTP_STATUS_CODE)
            if(awsCommonHelper.isResponseHttpStatusCodeSuccess(responeHttpStatusCode)):
                for topic in response[CONSTANTS.SNS.RESPONSE.TOPICS]:
                    topicArn = topic.get(CONSTANTS.SNS.RESPONSE.TOPIC_ARN)
                    if(topicArn.find(topicName) != -1):
                        result[CONSTANTS.CONTEXT.TOPIC_ARN] = topicArn
                        apiMgmt.setResultStatusSuccess(result)
                        logger.info(f"awsSnsHelper::getTopic() >> SNS Topic - Name: '{topicName}', TopicArn: '{topicArn}")

    except Exception as ex:
        apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())
        # logger.error(f"awsSnsHelper::getTopic() >> Exception has occurred. Error: '{str(ex)}'")

    return result


# def getTopic(topicName):
#     result = apiMgmt.getDefaultResult()

#     try:
#         if(topicName == None):
#             apiMgmt.setResultFailed(result, exception="Invalid Topic Name passed!")
#         else:
#             # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.ServiceResource.topics
#             topics = __sns.topics.all()
#             for topic in topics:
#                 if(topic.arn.find(topicName) != -1):
#                     result[CONSTANTS.CONTEXT.TOPIC_ARN] = topic.arn
#                     # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#topic
#                     result[CONSTANTS.CONTEXT.TOPIC_OBJECT] =  __sns.Topic(topic.arn)

#                     apiMgmt.setResultStatusSuccess(result)
#                     logger.info(f"awsSnsHelper::getTopic() >> SNS Topic - Name: '{topicName}'\
# , TopicArn: '{topic.attributes.get('TopicArn')}")
# #                     logger.info(f"awsSnsHelper::getTopic() >> SNS Topic - Name: '{topicName}'\
# # , TopicArn: '{topic.attributes.get('TopicArn')}'\
# # , DisplayName : '{topic.attributes.get('DisplayName')}'\
# # , DeliveryPolicy: '{topic.attributes.get('DeliveryPolicy')}'\
# # , EffectiveDeliveryPolicy : '{topic.attributes.get('EffectiveDeliveryPolicy')}'")

#     except Exception as ex:
#         apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())
#         # logger.error(f"awsSnsHelper::getTopic() >> Exception has occurred. Error: '{str(ex)}'")

#     return result



def publishMessage(topicName, messageBody, messageAttributes=None, subject=None):
    result = apiMgmt.getDefaultResult()

    try:
        resultGetTopic = getTopic(topicName)
        if(apiMgmt.isResultFailure(resultGetTopic)):
            result = resultGetTopic
        else:
            topicArn = resultGetTopic.get(CONSTANTS.CONTEXT.TOPIC_ARN)

            messageObject = {}

            messageObject[CONSTANTS.SNS.REQUEST.TARGET_ARN] = topicArn
            # Message Body
            messageObject[CONSTANTS.SNS.REQUEST.MESSAGE_BODY] = awsCommonHelper.getMessageJsonString(messageBody)
            # Subject
            if(subject):
                messageObject[CONSTANTS.SNS.REQUEST.SUBJECT] = subject
            # Message Attibutes
            if(messageAttributes):
                newNessageAttributes = awsCommonHelper.serializeDictToAttributeList(messageAttributes)
                messageObject[CONSTANTS.SNS.REQUEST.MESSAGE_ATTRIBUTE] = newNessageAttributes
            logger.info(f"awsSnsHelper::sendMessage() >> messageObject: {jsonHelper.convertObjectToFormattedJson(messageObject)}")

            result = __publishMessage(messageObject)

    except Exception as ex:
        apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())

    return result


def __publishMessage(messageObject):
    result = apiMgmt.getDefaultResult()

    try:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
        response = __snsClient.publish(**messageObject)
        logger.debug(f"awsSnsHelper::__publishMessage() >> response: {jsonHelper.convertObjectToFormattedJson(response)}")
        result[CONSTANTS.RESPONSE.MESSAGE_ID] = response.get(CONSTANTS.SNS.RESPONSE.MESSAGE_ID)
        responseMetaData = response.get(CONSTANTS.SNS.RESPONSE.RESPONSE_META_DATA)
        result[CONSTANTS.RESPONSE.PROVIDER_RESPONSE_DATA] = responseMetaData

        responeHttpStatusCode = responseMetaData.get(CONSTANTS.SNS.RESPONSE.HTTP_STATUS_CODE)
        if(awsCommonHelper.isResponseHttpStatusCodeSuccess(responeHttpStatusCode)):
            apiMgmt.setResultStatusSuccess(result)

    except Exception as ex:
        apiMgmt.setResultFailed(result, exception=str(ex), stackTrace=traceback.format_exc())
        # logger.error(f"awsSqsHelper::__sendMessage() >> Exception has occurred. Error: '{str(ex)}'")

    return result
