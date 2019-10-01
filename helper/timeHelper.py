# Standard library imports
# import os     # Used for Diagnostics
import time
import decimal
import logging
# Third party library imports
# Local application imports
from helper import constants
# Module Constants
# Module Level Variables
logger = logging.getLogger(__name__)    # Logger
timeLogDict = {}


# Diagnostics logs
# logger.debug(f"timeHelper::os.getcwd(): '{os.getcwd()}'")
# logger.debug(f"timeHelper::ModuleName:  '{__name__}'")
# logger.debug(f"timeHelper::dir():       {dir()}")


def convertDateTimeToString(datetime):
    return time.strftime(constants.DATETIME_STRING_FORMAT, datetime)

def convertStringToDateTime(datetimeString):
    return time.strptime(datetimeString, constants.DATETIME_STRING_FORMAT)


def getNow():
    """
    Returns seconds elapsed since epoch (1 January, 12:00 am, 1970)
    """
    return time.time()

def getUTCDateTime(timeSinceEpoch=None):
    """
    Returns struct_time. If time is provided it will use it, otherwise it uses current UTC time
    """
    return time.gmtime(timeSinceEpoch if timeSinceEpoch != None else getNow())

def getLocalDateTime(timeSinceEpoch=None):
    """
    Returns struct_time. If time is provided it will use it, otherwise it uses current time
    """
    return time.localtime(timeSinceEpoch if timeSinceEpoch != None else getNow())


def getUTCDateTimeString(timeSinceEpoch=None):
    """
    Return string representation for provided UTC time
    """
    return convertDateTimeToString(getUTCDateTime(timeSinceEpoch))

def getLocalDateTimeString(timeSinceEpoch=None):
    """
    Return string representation for provided time
    """
    return convertDateTimeToString(getLocalDateTime(timeSinceEpoch))


def startTimeLog(category, startTime=None):
    if(startTime == None):
        startTime = getNow()
    timeLogDict[category] = startTime
    return startTime

def endTimeLog(category, endTime=None):
    if(endTime == None):
        endTime = getNow()
    startTime = timeLogDict.get(category)
    return (startTime, endTime, (endTime - startTime) if startTime != None else None)

def endTimeLogString(category, endTime=None):
    timeLogTuple = endTimeLog(category, endTime)
    return getTimeLogTupleString(timeLogTuple[0], timeLogTuple[1])

def getTimeLogTuple(startTime, endTime=None):
    endTime = endTime if endTime != None else getNow()
    return (startTime, endTime, endTime - startTime)

def getTimeLogTupleString(startTime, endTime=None):
    timeLogTuple = getTimeLogTuple(startTime, endTime)
    return (getUTCDateTimeString(timeLogTuple[0]), getUTCDateTimeString(timeLogTuple[1]), decimal.Decimal(timeLogTuple[2]))

def sleep(timeToSleepInSeconds):
    """
    USE ONLY for TESTING
    """
    time.sleep(timeToSleepInSeconds)
