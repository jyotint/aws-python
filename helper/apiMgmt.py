# Standard library imports
# Third party library imports
# Local application imports


# NOTES for Developers: 
# 1. "result" must be defined before calling any methods from this module (with exception of "getDefaultResult()")
# 2. "result" must be a dictionary
# 3. Exception will be thrown if "result" is not defined or defined but not of dictionary type


class CONSTANTS:
    class RESULT:
        STATUS = "status"
        # DATA = "data"
        CODE = "code"
        MESSAGE = "message"
        ERROR = "error"
        ERROR_EXCEPTION = "exception"
        ERROR_STACK_TRACE = "stackTrace"

    class STATUS_CODE:
        SUCCESS = 0
        FAILED = -1

    FAILURE_STATUS_CODE_LIST = [
        STATUS_CODE.FAILED
    ]

    class MESSAGE:
        DEFAULT_ERROR_MESSAGE = "Unknown error has occured!"



def getDefaultResult():
    return {
        CONSTANTS.RESULT.STATUS: CONSTANTS.STATUS_CODE.FAILED
    }

def getResultStatus(result, defaultResultStatus=False):
    return result.get(CONSTANTS.RESULT.STATUS, defaultResultStatus)

def isResultSuccess(result):
    return getResultStatus(result) == CONSTANTS.STATUS_CODE.SUCCESS

def isResultFailure(result):
    return isStatusFailure(getResultStatus(result))

def isStatusFailure(status):
    return (status in CONSTANTS.FAILURE_STATUS_CODE_LIST)


# def getResultData(result):
#     return result.get(CONSTANTS.RESULT.DATA)

def getResultError(result):
    return result.get(CONSTANTS.RESULT.ERROR, {})

def getResultErrorException(result):
    errorDict = result.get(CONSTANTS.RESULT.ERROR, {})
    return errorDict.get(CONSTANTS.RESULT.ERROR_EXCEPTION)

def getResultErrorStackTrace(result):
    errorDict = result.get(CONSTANTS.RESULT.ERROR, {})
    return errorDict.get(CONSTANTS.RESULT.ERROR_STACK_TRACE)


def setResultStatusSuccess(result):
    setResultStatus(result, CONSTANTS.STATUS_CODE.SUCCESS)

def setResultStatusFailed(result):
    setResultStatus(result, CONSTANTS.STATUS_CODE.FAILED)

def setResultStatus(result, status):
    result[CONSTANTS.RESULT.STATUS] = status

def setResultSuccess(result, code=None, message=None, exception=None, stackTrace=None):
    setResult(result, status=CONSTANTS.STATUS_CODE.SUCCESS, code=code, message=message, exception=exception, stackTrace=stackTrace)

def setResultFailed(result, code=None, message=None, exception=None, stackTrace=None):
    setResult(result, status=CONSTANTS.STATUS_CODE.FAILED, code=code, message=message, exception=exception, stackTrace=stackTrace)

def setResult(result, status=None, code=None, message=None, exception=None, stackTrace=None):
    if(status):
        setResultStatus(result, status)
    if(code):
        result[CONSTANTS.RESULT.CODE] = code
    if(message):
        result[CONSTANTS.RESULT.MESSAGE] = message
    if(exception or stackTrace):
        errorDict = result.setdefault(CONSTANTS.RESULT.ERROR, {})
        if(exception):
            errorDict[CONSTANTS.RESULT.ERROR_EXCEPTION] = exception
        if(stackTrace):
            errorDict[CONSTANTS.RESULT.ERROR_STACK_TRACE] = stackTrace


def getPrintableMessage(result):
    status = getResultStatus(result)
    code = result.get(CONSTANTS.RESULT.CODE)
    message = result.get(CONSTANTS.RESULT.MESSAGE)

    msg = f"Status: '{status}'"
    if(code):
        msg = f"{msg}, Code: {code}"
    if(message):
        msg = f"{msg}, Message: '{message}''"

    # TODO: Proper formatting using new line characters, etc.
    errorDict = result.get(CONSTANTS.RESULT.ERROR)
    if(errorDict):
        exception = errorDict.get(CONSTANTS.RESULT.ERROR_EXCEPTION)
        stackTrace = errorDict.get(CONSTANTS.RESULT.ERROR_STACK_TRACE)
        if(exception):
            msg = f"{msg}, Exception: {exception}"
        if(stackTrace):
            msg = f"{msg}, StackTrace: '{stackTrace}''"

    return msg


# def composeError(errorMessage=None, errorCode=None):
#     result = {}
#     if(errorCode):
#         result[CONSTANTS.RESULT.CODE] = errorCode
#     if(errorMessage):
#         result[CONSTANTS.RESULT.MESSAGE] = errorMessage
#     else:
#         result[CONSTANTS.RESULT.MESSAGE] = CONSTANTS.MESSAGE.DEFAULT_ERROR_MESSAGE
#     return result


# def composeResult(status, data=None, error=None, errorMessage=None, errorCode=None):
#     result = {}
#     result[CONSTANTS.RESULT.STATUS] = status
#     if(data):
#         result[CONSTANTS.RESULT.DATA] = data
#     if(isStatusFailure(status)):
#         if(error):
#             result[CONSTANTS.RESULT.ERROR] = error
#         else:
#             result[CONSTANTS.RESULT.ERROR] = composeError(errorMessage=errorMessage, errorCode=errorCode)
#     return result
