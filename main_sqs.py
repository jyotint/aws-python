# Standard library imports
# import os     # Used for Diagnostics
import sys
sys.path.append("..")
import argparse
import traceback
# Third party library imports
# Local application importsimport os
from helper import timeHelper, jsonHelper
from awsHelper import awsSqsHelper


# Module Constants


# Diagnostics logs
# print(f"main_sqs::os.getcwd(): '{os.getcwd()}'")
# print(f"main_sqs::ModuleName:  '{__name__}'")
# print(f"main_sqs::dir():       {dir()}")


def setupAndParseCommandLine(args): 
    print(f"main_sqs::setupAndParseCommandLine() >> run: {args}")
    parser = argparse.ArgumentParser(description = "Posts data to AWS SQS")
    parser.add_argument("qn", metavar="QueueName", help="AWS SQS Queue Name")
    parser.add_argument("-v", "--version", action="version", version="%(prog)s 1.0.0")
    return parser.parse_args(args)


def main(args):
    try:
        arguments = setupAndParseCommandLine(args)
        queueName = arguments.qn

        # Test Scenario 01: None
        # result = awsSqsHelper.postMessage(queueName, message=None)
        
        # Test Scenario 02: Tuple type
        # result = awsSqsHelper.postMessage(queueName, message=("a",2))

        # Test Scenario 03: String type
        # result = awsSqsHelper.postMessage(queueName, message=f"Hi There (sent at {timeHelper.getUTCDateTimeString()})")

        # Test Scenario 04: Dictionary type
        message = {}
        message["message"] = "Hello World!"
        message["SentAt"] = timeHelper.getUTCDateTimeString()
        result = awsSqsHelper.postMessage(queueName, message)

        # print("main_sqs::main() >> result: ", result)
        print("main_sqs::main() >> result: ", jsonHelper.getFormattedJson(result))
        if(result.get(awsSqsHelper.CONTANTS.RESPONSE.STATUS) == False):
            print(result.get(awsSqsHelper.CONTANTS.RESPONSE.STACK_TRACE))

    except Exception as ex:
        print(f"main_sqs::main() >> Exception has occurred. Error: '{ex.__str__()}'")
        print(traceback.format_exc())


if( __name__ == "__main__"):
    main(sys.argv[1:])
