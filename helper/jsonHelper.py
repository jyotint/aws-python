# Standard library imports
import json
# Third party library imports
# Local application importsimport os


# Module Constants
class CONSTANTS:
    FORMATTED_JSON_INDENT = 2



def convertJsonToObject(jsonString):
    return jsonString if jsonString == None else json.loads(jsonString)


def convertObjectToJson(obj):
    return obj if obj == None else json.dumps(obj)


def convertObjectToFormattedJson(obj, indentValue=CONSTANTS.FORMATTED_JSON_INDENT):
    return obj if obj == None else json.dumps(obj, indent=indentValue)
