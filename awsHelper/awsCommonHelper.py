# Standard library imports
import traceback
import logging
# import os     # Used for Diagnostics
# Third party library imports
# Local application imports
from helper import jsonHelper


class CONSTANTS:
    SUCCESS_HTTP_STATUS_CODE_LIST = [
        200
    ]


def isResponseHttpStatusCodeSuccess(httpStatusCode, httpStatusCodeList=CONSTANTS.SUCCESS_HTTP_STATUS_CODE_LIST):
    return httpStatusCode in httpStatusCodeList


def getAwsObjectBooleanAttributeValue(awsObj, attr, defaultValue=False):
    value = awsObj.attributes.get(attr, defaultValue)
    if(type(value) == type('')):
        if(value == 'true'):
            value = True
        elif(value == 'false'):
            value = False
    return value


def getMessageJsonString(data):
    # logger.debug(f"awsCommonHelper::getMessageJsonString() >> Message type '{type(data)}'")
    messageString = None
    if(type(data) == type({})):
        messageString = jsonHelper.convertObjectToJson(data)
    elif(type(data) == type("")):
        messageString = data
    else:
        raise Exception(f"Unsupported message type pass, only 'String (containing JSON)' and 'Dictionary' type are supported (passed type: '{type(data)}')")

    return messageString


def serializeGetAttributeValue(value):
    valueAttr = {}
    valueAttr["DataType"] = "String"
    valueAttr["StringValue"] = value
    return valueAttr

def deserializeGetAttributeValue(valueAttr):
    return valueAttr["StringValue"]

def serializeDictToAttributeList(datDict):
    newMessageAttributes = {}
    if(datDict):
        for key, value in datDict.items():
            newMessageAttributes[key] = serializeGetAttributeValue(value)
    return newMessageAttributes

def deserializeAttributeLisToDict(messageAttributes):
    dataDict = {}
    if(messageAttributes):
        for key, value in messageAttributes.items():     
            dataDict[key] = deserializeGetAttributeValue(value)
    return dataDict
