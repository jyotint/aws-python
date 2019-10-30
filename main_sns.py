# Standard library imports
import os     # Used for Diagnostics
import sys
sys.path.append("..")
import time
import uuid
# import pprint
import click
import traceback
import logging
import coloredlogs
# Third party library imports
# Local application importsimport os
from helper import timeHelper
from helper import jsonHelper
from helper import apiMgmt
from helper import configureLogging
from awsHelper import awsCommonHelper
from awsHelper import awsSnsHelper
# Module Constants


# Module level variables and code
configureLogging.setupLogging()
logger = logging.getLogger(__name__)
coloredlogs.install()
# Diagnostics logs
# pp = pprint.PrettyPrinter(indent=2)
# if("os" in dir()):
#     logger.info(f"main_sns::os.getcwd(): '{os.getcwd()}'")
# logger.info(f"main_sns::ModuleName:  '{__name__}'")
# logger.info(f"main_sns::dir():       {dir()}")


@click.group()
def main():
    pass


@main.command("pm", help="Publish message(s) to a SNS topic")
@click.argument("TopicName", type=click.STRING)    # help="SNS Topic (name) to publish message to")
@click.option("-c", "--count", type=click.INT, default=1, help="Number of mesages to send. Defaults to 1.")
@click.option("-i", "--interval", type=click.INT, default=2, help="Duration between messages in Seconds. Defaults to 2.")
def publishMessages(topicname, count, interval):
    publishMessagesLL(topicname, count=count, interval=interval)


@main.command("sm", help="Subscribe to message(s) for a SNS topic")
@click.argument("TopicName", type=click.STRING)    # help="SNS Topic (name) to subscribe to")
@click.option("-c", "--count", type=click.INT, default=1, help="Number of mesages to send. Defaults to 1.")
@click.option("-i", "--interval", type=click.INT, default=2, help="Duration between messages in Seconds. Defaults to 2.")
def subscribeToTopic(topicname, count, interval):
    pass


def publishMessagesLL(topicName, count=1, interval=2):
    logger.info(f"main_sns::publishMessagesLL() >> Parameters >> topicName: '{topicName}', count: {count}, interval: {interval} second(s)")
    successCount = 0

    for i in range(1, count+1):
        if(i != 1):
            time.sleep(interval)

        logger.info(f"main_sns::publishMessagesLL() >>   Publishing message #{i}...")

        currentDateTime = timeHelper.getUTCDateTimeString()
        messageBody = getDefaultMessageBody(modifiedAt=currentDateTime)
        messageAttributes = getMessageAttributes("HelloWorldType", currentDateTime=currentDateTime)

        subject = f"Message #{i} published on '{topicName}' AWS SNS topic at {currentDateTime}"
        messageBody["body"] = f"Hello World #{i} sent at {currentDateTime}."

        result = awsSnsHelper.publishMessage(
            topicName, 
            messageBody, 
            messageAttributes=messageAttributes,
            subject=subject)

        logger.info(f"main_sns::publishMessagesLL() >> result: {jsonHelper.convertObjectToFormattedJson(result)}")
        if(apiMgmt.isResultFailure(result)):
            logger.error(apiMgmt.getResultErrorStackTrace(result))
        else:
            successCount += 1
            logger.info(f"main_sns::publishMessagesLL() >>     Published successfully.")
        
    logger.info(f"main_sns::publishMessagesLL() >> Successfully published {successCount} of {count} message(s).")


def getDefaultMessageBody(modifiedBy="Jyotindra", modifiedAt=None):
    messageBody = {}
    messageBody["modifiedBy"] = modifiedBy
    messageBody["modifiedAt"] = modifiedAt if modifiedAt else timeHelper.getUTCDateTimeString()
    return messageBody


def getMessageAttributes(messageType, messageVersion="1.0", messageFormat="application/json", currentDateTime=None):
    messageAttributes = {}
    messageAttributes["messageVersion"] = messageVersion
    messageAttributes["messageFormat"] = messageFormat
    messageAttributes["messageType"] = messageType
    messageAttributes["messageSentAt"] = currentDateTime if currentDateTime else timeHelper.getUTCDateTimeString()
    return messageAttributes


if( __name__ == "__main__"):
    try:
        main()
    except Exception as ex:
        logger.error(f"main_sns::main() >> Exception has occurred. Error: '{ex.__str__()}'", exc_info=True)
        # logger.error(traceback.format_exc())
