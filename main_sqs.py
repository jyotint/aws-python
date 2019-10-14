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
from awsHelper import awsSqsHelper
# Module Constants


# Module level variables and code
configureLogging.setupLogging()
logger = logging.getLogger(__name__)
coloredlogs.install()
# Diagnostics logs
# pp = pprint.PrettyPrinter(indent=2)
# if("os" in dir()):
#     logger.info(f"main_sqs::os.getcwd(): '{os.getcwd()}'")
# logger.info(f"main_sqs::ModuleName:  '{__name__}'")
# logger.info(f"main_sqs::dir():       {dir()}")


@click.group()
def main():
    pass


@main.command("rm", help="Receive messages")
@click.argument("QueueName", type=click.STRING) # help="SQS queue (name) to recieve message from")
@click.option("-c", "--count", type=click.INT, default=10, help="Number of times to loop for receiving messages. Defaults to 10.")
@click.option("-w", "--wait", type=click.INT, default=5, help="Number of seconds to wait between each message. Simulate long message processing time. Defaults to 5.")
def receiveMessages(queuename, count, wait):
    receiveMessagesLL(queuename, count, wait)


@main.command("sms", help="Send standard messages")
@click.argument("QueueName", type=click.STRING)    # help="SQS queue (name) to send message to")
@click.option("-c", "--count", type=click.INT, default=1, help="Number of mesages to send. Defaults to 1.")
@click.option("-i", "--interval", type=click.INT, default=2, help="Duration between messages in Seconds. Defaults to 2.")
def sendStandardMessages(queuename, count, interval):
    sendMessagesLL(queuename, count=count, interval=interval)


@main.command("smf", help="Send FIFO messages")
@click.argument("QueueName", type=click.STRING)    # help="SQS queue (name) to send message to")
@click.option("-c", "--count", type=click.INT, default=1, help="Number of mesages to send. Defaults to 1.")
@click.option("-i", "--interval", type=click.INT, default=2, help="Duration between messages in Seconds. Defaults to 2.")
@click.option("-g", "--samegroup", type=click.BOOL, is_flag=True, default=False, help="Use same UUID for all messages in group. Applicable for FIFO Queue. Defaults to False.")
@click.option("-d", "--samededup", type=click.BOOL, is_flag=True, default=False, help="Use same UUID for all messages for deduplication. Applicable for FIFO Queue. Defaults to False.")
def sendFifoMessages(queuename, count, interval, samegroup, samededup):
    sendMessagesLL(queuename, count=count, interval=interval, useSameGroupId=samegroup, useSameDeduplicationId=samededup)


def sendMessagesLL(queueName, count=1, interval=2, useSameGroupId=False, useSameDeduplicationId=False):
    # Test Scenario 01: None
    # result = awsSqsHelper.sendMessage(queueName, message=None)

    # Test Scenario 02: Tuple type
    # result = awsSqsHelper.sendMessage(queueName, message=("a",2))

    # Test Scenario 03: String type
    # result = awsSqsHelper.sendMessage(queueName, message=f"Hi There (sent at {timeHelper.getUTCDateTimeString()})")

    # Test Scenario 04: Dictionary type
    successCount = 0
    messageGroupId = None
    messageDeduplicationId = None
    logger.info(f"main_sqs::sendMessagesLL() >> Paramters >> queueName: '{queueName}', count: {count}, interval: {interval} second(s), useSameGroupId: {useSameGroupId}, useSameDeduplicationId: {useSameDeduplicationId}")

    if(useSameGroupId):
        messageGroupId = str(uuid.uuid4())
    if(useSameDeduplicationId):
        messageDeduplicationId = str(uuid.uuid4())

    for i in range(1, count+1):
        if(i != 1):
            time.sleep(interval)

        logger.info(f"main_sqs::sendMessagesLL() >>   Sending message #{i}...")

        currentDateTime = timeHelper.getUTCDateTimeString()
        messageBody = getDefaultMessageBody(modifiedAt=currentDateTime)
        messageAttributes = getMessageAttributes("HelloWorldType", currentDateTime=currentDateTime)

        messageBody["body"] = "Hello World!"

        result = awsSqsHelper.sendMessage(
            queueName, 
            messageBody, 
            messageAttributes=messageAttributes, 
            messageGroupId=messageGroupId, 
            messageDeduplicationId=messageDeduplicationId)

        # logger.debug(f"main_sqs::sendMessagesLL() >> result: {result}")
        logger.debug(f"main_sqs::sendMessagesLL() >> result: {jsonHelper.convertObjectToFormattedJson(result)}")
        if(apiMgmt.isResultFailure(result)):
            logger.error(result.get(apiMgmt.CONSTANTS.RESULT.STACK_TRACE))
        else:
            successCount += 1
            logger.info(f"main_sqs::sendMessagesLL() >>     Sent successfully.")
        
    logger.info(f"main_sqs::sendMessagesLL() >> Successfully sent {successCount} of {count} message(s).")


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


def receiveMessagesLL(queuename, count=10, wait=5):
    queueName = queuename
    successCount = receivedCount = 0
    logger.info(f"main_sqs::receiveMessagesLL() >> Paramters >> queueName: '{queueName}', count: {count}, wait: {wait} second(s)")
    for i in range(1, count+1):
        result = awsSqsHelper.receiveMessages(queueName, messageProcessorFunc=receiveMessageProcessor)
        logger.debug(f"receiveMessagesLL result: {result}")
        if(apiMgmt.isResultSuccess(result)):
            messages = result[awsSqsHelper.CONSTANTS.CONTEXT.MESSAGES]
            receivedCount += len(messages)
            logger.info(f"main_sqs::receiveMessagesLL() >>   Received {len(messages)} message(s). Processing messages...")
            for index, message in enumerate(messages):
                resultProcessor = receiveMessageProcessor(message, index)
                time.sleep(wait)
                if(resultProcessor):
                    logger.info(f"main_sqs::receiveMessagesLL() >>     Message processed successfully.")
                    successCount += 1
                    awsSqsHelper.deleteMessage(message)
                else:
                    logger.error(f"main_sqs::receiveMessagesLL() >>     Message processing FAILED.")
            logger.info("")
    logger.info(f"main_sqs::receiveMessagesLL() >> Successfully processed {successCount} of {receivedCount} received message(s).")


def receiveMessageProcessor(message, index):
    result = True
    logger.info(f"main_sqs::receiveMessageProcessor() >> Processing message #{index+1}...")

    messageBody = jsonHelper.convertJsonToObject(message.body)
    messageAttributes = awsSqsHelper.deserializeAttributeLisToDict(message.message_attributes)

    logger.info(f"    Message Id:             {message.message_id}")
    logger.info(f"    Message Attributes:     {jsonHelper.convertObjectToFormattedJson(message.attributes)}")
    logger.info(f"    User Defined Body:      {jsonHelper.convertObjectToFormattedJson(messageBody)}")
    logger.info(f"    User DefinedAttributes: {jsonHelper.convertObjectToFormattedJson(messageAttributes)}")
    
    return result


if( __name__ == "__main__"):
    try:
        main()
    except Exception as ex:
        logger.error(f"main_sqs::main() >> Exception has occurred. Error: '{ex.__str__()}'", exc_info=True)
        # logger.error(traceback.format_exc())
