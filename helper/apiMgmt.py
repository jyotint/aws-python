# Standard library imports
# Third party library imports
# Local application imports


class CONSTANTS:
    class RESULT:
        STATUS = "status"
        # DATA = "data"
        ERROR = "error"
        ERROR_CODE = "code"
        ERROR_MESSAGE = "message"
        EXCEPTION = "Exception"
        STACK_TRACE = "StackTrace"

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

def getResultStatus(result):
    return result.get(CONSTANTS.RESULT.STATUS)

def isResultSuccess(result):
    return getResultStatus(result) == CONSTANTS.STATUS_CODE.SUCCESS if result != None else False

def isStatusFailure(status):
    return (status in CONSTANTS.FAILURE_STATUS_CODE_LIST) if status != None else False

def isResultFailure(result):
    return isStatusFailure(getResultStatus(result))

# def getResultData(result):
#     return result.get(CONSTANTS.RESULT.DATA) if result != None else None

def getResultError(result):
    return result.get(CONSTANTS.RESULT.ERROR) if result != None else None

def setResultStatusSuccess(result):
    result[CONSTANTS.RESULT.STATUS] = CONSTANTS.STATUS_CODE.SUCCESS

def setResultStatusFailed(result):
    result[CONSTANTS.RESULT.STATUS] = CONSTANTS.STATUS_CODE.FAILED

def setResultStatus(result, status):
    result[CONSTANTS.RESULT.STATUS] = status

def setResultSuccess(result, exception=None, stackTrace=None):
    setResult(result, status=CONSTANTS.STATUS_CODE.SUCCESS, exception=exception, stackTrace=stackTrace)

def setResultFailed(result, exception=None, stackTrace=None):
    setResult(result, status=CONSTANTS.STATUS_CODE.FAILED, exception=exception, stackTrace=stackTrace)

def setResult(result, status=None, exception=None, stackTrace=None):
    if(status != None):
        setResultStatus(result, status)
    if(exception != None):
        result[CONSTANTS.RESULT.EXCEPTION] = exception
    if(stackTrace != None):
        result[CONSTANTS.RESULT.STACK_TRACE] = stackTrace


def getErrorPrintMessage(result):
    status = getResultStatus(result)
    error = result.get(CONSTANTS.RESULT.ERROR)
    errorCode = error.get(CONSTANTS.RESULT.ERROR_CODE)
    errorMessage = error.get(CONSTANTS.RESULT.ERROR_MESSAGE)
    msg = f"Status: '{status}'"
    if(error != None):
        if(errorCode != None):
            msg = f"{msg}, Error Code: {errorCode}"
        if(errorMessage != None):
            msg = f"{msg}, Error Message: '{errorMessage}''"

    return msg


def composeError(errorMessage=None, errorCode=None):
    result = {}
    if(errorCode != None):
        result[CONSTANTS.RESULT.ERROR_CODE] = errorCode
    result[CONSTANTS.RESULT.ERROR_MESSAGE] = errorMessage if errorMessage != None else CONSTANTS.MESSAGE.DEFAULT_ERROR_MESSAGE
    return result

# def composeResult(status, data=None, error=None, errorMessage=None, errorCode=None):
#     result = {}
#     result[CONSTANTS.RESULT.STATUS] = status
#     if(data != None):
#         result[CONSTANTS.RESULT.DATA] = data
#     if(isStatusFailure(status)):
#         if(error != None):
#             result[CONSTANTS.RESULT.ERROR] = error
#         else:
#             result[CONSTANTS.RESULT.ERROR] = composeError(errorMessage=errorMessage, errorCode=errorCode)
#     return result
