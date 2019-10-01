# Standard library imports
import os     # Used for Diagnostics
import sys
sys.path.append("..")
import time
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


@main.command("sm", help="Send messages")
@click.argument("qn", type=click.STRING)    # help="SQS queue (name) to send message to")
@click.option("-c", "--count", type=click.INT, default=1, help="Number of mesages to send. Defaults to 1.")
@click.option("-i", "--interval", type=click.INT, default=2, help="Duration between messages in Seconds. Defaults to 2.")
def sendMessages(qn, count, interval):
    queueName = qn
    
    # Test Scenario 01: None
    # result = awsSqsHelper.sendMessage(queueName, message=None)

    # Test Scenario 02: Tuple type
    # result = awsSqsHelper.sendMessage(queueName, message=("a",2))

    # Test Scenario 03: String type
    # result = awsSqsHelper.sendMessage(queueName, message=f"Hi There (sent at {timeHelper.getUTCDateTimeString()})")

    # Test Scenario 04: Dictionary type
    successCount = 0
    logger.info(f"main_sqs::sendMessages() >> Paramters >> count: {count}, interval: {interval}, queueName: '{queueName}'")
    for i in range(1, count+1):
        if(i != 1):
            time.sleep(interval)

        logger.info(f"main_sqs::sendMessages() >>   Sending message #{i}...")
        currentDateTime = timeHelper.getUTCDateTimeString()

        messageBody = {}
        messageBody["body"] = "Hello World!"
        messageBody["modifiedBy"] = "Jyotindra"
        messageBody["modifiedAt"] = currentDateTime

        messageAttributes = {}
        messageAttributes["messageVersion"] = "1.0"
        messageAttributes["messageFormat"] = "application/json"
        messageAttributes["messageType"] = "HelloWorldType"
        messageAttributes["messageSentAt"] = currentDateTime
        
        result = awsSqsHelper.sendMessage(queueName, messageBody, messageAttributes=messageAttributes)

        # logger.debug(f"main_sqs::sendMessages() >> result: {result}")
        logger.debug(f"main_sqs::sendMessages() >> result: {jsonHelper.convertObjectToFormattedJson(result)}")
        if(apiMgmt.isResultFailure(result)):
            logger.error(result.get(apiMgmt.CONSTANTS.RESULT.STACK_TRACE))
        else:
            successCount += 1
            logger.info(f"main_sqs::sendMessages() >>     Sent successfully.")
        
    logger.info(f"main_sqs::sendMessages() >> Successfully sent {successCount} of {count} message(s).")


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


@main.command("rm", help="Receive messages")
@click.argument("qn", type=click.STRING) # help="SQS queue (name) to recieve message from")
@click.option("-c", "--count", type=click.INT, default=10, help="Number of times to loop for receiving messages. Defaults to 10.")
@click.option("-w", "--wait", type=click.INT, default=5, help="Number of seconds to wait between each message. Simulate long message processing time. Defaults to 5.")
def receiveMessages(qn, count, wait):
    queueName = qn
    successCount = receivedCount = 0
    logger.info(f"main_sqs::receiveMessages() >> Paramters >> count: {count}, wait: {wait}, queueName: '{queueName}'")
    for i in range(1, count+1):
        result = awsSqsHelper.receiveMessages(queueName, messageProcessorFunc=receiveMessageProcessor)
        logger.debug(f"receiveMessages result: {result}")
        if(apiMgmt.isResultSuccess(result)):
            messages = result[awsSqsHelper.CONSTANTS.CONTEXT.MESSAGES]
            receivedCount += len(messages)
            logger.info(f"main_sqs::receiveMessages() >>   Received {len(messages)} message(s). Processing messages...")
            for index, message in enumerate(messages):
                resultProcessor = receiveMessageProcessor(message, index)
                time.sleep(wait)
                if(resultProcessor):
                    logger.info(f"main_sqs::receiveMessages() >>     Message processed successfully.")
                    successCount += 1
                    awsSqsHelper.deleteMessage(message)
                else:
                    logger.error(f"main_sqs::receiveMessages() >>     Message processing FAILED.")
            logger.info("")
    logger.info(f"main_sqs::receiveMessages() >> Successfully processed {successCount} of {receivedCount} received message(s).")


if( __name__ == "__main__"):
    try:
        main()
    except Exception as ex:
        logger.error(f"main_sqs::main() >> Exception has occurred. Error: '{ex.__str__()}'", exc_info=True)
        # logger.error(traceback.format_exc())
