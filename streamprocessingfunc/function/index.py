"""
Lambda Function for processing the DynamoDB Stream of OptionsHist Table

Function is invoked from an EventSourceMapping from to DynamoDB OptionsHist
Table. Responible for maintaining information about what data is in the
OptionsHist table.
"""


import os
import boto3
from datetime import datetime

if not os.environ.get("XRAYACTIVATED") is None:
    # Xray Has been activated for the stack, patch calls to AWS services
    #  with X-Ray Trace Headers and send trace segments to X-Ray deamon
    #  from aws_xray_sdk.core import xray_recorder
    from aws_xray_sdk.core import patch_all
    # Patch for X-Ray Tracing to DynamoDB API Calls
    patch_all()

# Initialize the DynamoDB OptionsHistTickers Table resource
ticker_table = boto3.resource('dynamodb').Table('OptionsHistTickers')


def _error_logging(ticker, error):
    """
    Help Funtion for consistent Logging for formats for CloudWatch
    Not Implemented Yet
    """
    pass


def getTickerDayTuples(record):
    if record['eventName'] != 'INSERT':
        # For now only using INSERT events to keep OptionsHistTickers
        #  metadata up todate. Just dropping everything else
        return None

    keys = record['dynamodb']['Keys']
    ticker = keys['Ticker']['S']
    day = str(keys['TimeCollectedExpirationDate']['N'])[:8]
    return (ticker, day)


def handler(event, context):
    """
    Lambda handler function,
    Scan DynamoDB Stream events from OptionsHist table,
        and maintain data in OptionsHistTickers
    """

    records = event.get('Records')
    if records is None:
        return 0
    # Parse Records to find which (ticker, day) combinations are in the
    #  set of records
    pairs = set(map(getTickerDayTuples, records))

    try:
        # remove None in the pair set if it exists
        pairs.remove(None)
    except KeyError:
        # Catch exception is none doesn't exist in pairs set
        pass

    for ticker, day in pairs:
        item = ticker_table.get_item(
            Key={
                'Ticker': ticker,
                'Collecting': 'TRUE'
            }
            ).get('Item')

        if item is None:
            # Check if ticker has been removed from the collect list
            #  between collect and processing of the stream
            continue

        day = datetime.strptime(day, '%Y%m%d').isoformat()[:10]

        if item.get('Starting') is None:
            # Starting attribute in None set initial Starting and
            #  Ending Attributes
            ticker_table.update_item(
                Key={'Ticker': ticker, 'Collecting': 'TRUE'},
                UpdateExpression='SET Starting = :start, Ending = :end',
                ExpressionAttributeValues={
                    ':start': day,
                    ':end': day
                    },
                ConditionExpression='attribute_exists(Collecting)'
                )
        elif day > item['Ending']:
            # Update the Ending day for the company in OptionsHistTickers
            #  table
            ticker_table.update_item(
                Key={'Ticker': ticker, 'Collecting': 'TRUE'},
                UpdateExpression='SET Ending = :end',
                ExpressionAttributeValues={
                    ":end": day
                    },
                ConditionExpression='attribute_exists(Collecting)'
                )

    return 0
