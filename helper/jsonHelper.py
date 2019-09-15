# Standard library imports
import json
# Third party library imports
# Local application importsimport os


# Module Constants
class CONTANTS:
    FORMATTED_JSON_INDENT = 2



def getFormattedJson(data, indentValue=CONTANTS.FORMATTED_JSON_INDENT):
    return data if data == None else json.dumps(data, indent=indentValue)


def convertToString(data):
    return data if data == None else json.dumps(data)
