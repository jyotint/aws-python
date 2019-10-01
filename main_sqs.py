# Standard library imports
import os     # Used for Diagnostics
import sys
sys.path.append("..")
import time
import pprint
import argparse
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


def setupAndParseCommandLine(args): 
    logger.info(f"main_sqs::setupAndParseCommandLine() >> run: {args}")
    logger.info("")
    parser = argparse.ArgumentParser(description = "Posts data to AWS SQS")
    parser.add_argument("command", metavar="Command", choices=["sm", "rm"], help="Command List")
    parser.add_argument("qn", metavar="QueueName", help="AWS SQS Queue Name")
    parser.add_argument("-v", "--version", action="version", version="%(prog)s 1.0.0")
    return parser.parse_args(args)


def runApplication(args):
    try:
        arguments = setupAndParseCommandLine(args)
        queueName = arguments.qn
        commandName = arguments.command

        if(commandName == "sm"):
            testSendMessage(queueName)
        elif(commandName == "rm"):
            testReceiveMessages(queueName)
        else:
            logger.error(f"Invalid '{commandName}' command!")

    except Exception as ex:
        logger.error(f"main_sqs::runApplication() >> Exception has occurred. Error: '{ex.__str__()}'", exc_info=True)
        # logger.error(traceback.format_exc())


def testSendMessage(queueName):
    # Test Scenario 01: None
    # result = awsSqsHelper.sendMessage(queueName, message=None)

    # Test Scenario 02: Tuple type
    # result = awsSqsHelper.sendMessage(queueName, message=("a",2))

    # Test Scenario 03: String type
    # result = awsSqsHelper.sendMessage(queueName, message=f"Hi There (sent at {timeHelper.getUTCDateTimeString()})")

    # Test Scenario 04: Dictionary type
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

    # logger.debug(f"main_sqs::testSendMessage() >> result: {result}")
    logger.info(f"main_sqs::testSendMessage() >> result: {jsonHelper.convertObjectToFormattedJson(result)}")
    if(apiMgmt.isResultFailure(result)):
        logger.error(result.get(apiMgmt.CONSTANTS.RESULT.STACK_TRACE))


def testReceiveMessageProcessor(message, index):
    result = True
    logger.info(f"main_sqs::testReceiveMessages() >> Processing message #{index+1}...")

    messageBody = jsonHelper.convertJsonToObject(message.body)
    messageAttributes = awsSqsHelper.deserializeAttributeLisToDict(message.message_attributes)

    logger.info(f"    Message Id:             {message.message_id}")
    logger.info(f"    Message Attributes:     {jsonHelper.convertObjectToFormattedJson(message.attributes)}")
    logger.info(f"    User Defined Body:      {jsonHelper.convertObjectToFormattedJson(messageBody)}")
    logger.info(f"    User DefinedAttributes: {jsonHelper.convertObjectToFormattedJson(messageAttributes)}")
    
    time.sleep(2)      # Sleep to simulate long processing
    if(result):
        logger.info(f"main_sqs::testReceiveMessages() >>   Message processed successfully.")
    else:
        logger.error(f"main_sqs::testReceiveMessages() >>   Message processing FAILED.")

    logger.info("")
    return result


def testReceiveMessages(queueName):
    result = awsSqsHelper.receiveMessages(queueName, messageProcessorFunc=testReceiveMessageProcessor)
    # logger.info(f"testReceiveMessages result: {result}")
    if(apiMgmt.isResultSuccess(result)):
        messages = result[awsSqsHelper.CONSTANTS.CONTEXT.MESSAGES]
        logger.info(f"main_sqs::testReceiveMessages() >> {len(messages)} message(s) received.")
        for index, message in enumerate(messages):
            resultProcessor = testReceiveMessageProcessor(message, index)
            if(resultProcessor == True):
                awsSqsHelper.deleteMessage(message)


if( __name__ == "__main__"):
    runApplication(sys.argv[1:])
