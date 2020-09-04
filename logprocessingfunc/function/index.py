"""
Inputs Events:
    -------------------------------------------------------------------
    Any invokation of the this funcion produces the same result

This function always processes through the logs upto when the last
"--Init Collect Data" it found in the log events. This is a tight
coupling to the collectdatafunc lambda function of this project.
The Lambda function was written with the intention of being called
from the collectdatafunc at the end of each collection run.

The function Processes through the logs and updates collection
Error statistics on in the "OptionsHistTickers" Table. Then publishes
information to a SNS Topic

Any CRITIAL ERRORs are always published to SNS.

The Function "is_flagged" below is where the logic for determining what
is and what won't be publish to SNS when it comes to scaping and other
errors. The email the report gets published to should be defined in
deploy.sh before it is ran to launch the stack

Notes on the Errors that are being logged with in collectdatafunc Lambda
Function.

Log Events
    -------------------------------------------------------------------
    CRITCIAL ERRORs
    [NONE] [NONE] [""] --> Failed to get the collection ticker list
        comes from initial process of data collection error accured
        when trying to get the list of tickers to collect.
    [DYNAMODB] [NONE] [?Exception] --> Collection Item was constructed
        but faile to be "put" into DYNAMODB

    SCRAPING ERRORS
    [ticker] [NONE] ["No Expiration Dates"] --> Logged when the intital
        endpoint for getting the list of expirattion dates for a given
        ticker fails to return a list of expiration dates
    [ticker] [expiration_date] ['No Calls Data'] --> The web scraping
        stage was unable to locate a Calls table
    [ticker] [expiration_date] ['No Puts Data'] --> The web scraping
        stage was unable to loacte a Puts table
    [ticker] [expiration_date] ['No Data'] --> The web scraping stage
        was unable to find either table of calls or puts

    OTHER ERRORS
    [ticker] [expiration_date] [?Exception] --> These are thrown for
        an assorment of reasons. From failing to encode the table for
        dynamodb or other errors in the scraping process other then
        specific ones list above
"""

import boto3
import re
import json
import os
from decimal import Decimal
import logging

if not os.environ.get("XRAYACTIVATED") is None:
    # Xray Has been activated for the stack, patch calls to AWS services
    #  with X-Ray Trace Headers and send trace segments to X-Ray deamon
    #  from aws_xray_sdk.core import xray_recorder not currently used
    from aws_xray_sdk.core import patch_all
    # Patch for X-Ray Tracing to AWS Service API calls
    patch_all()

# Set up the logging module for function
logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ['LOGLEVEL']))

# Pull the SNS TOPIC ARN in from the enviorment
SNSTOPICARN = os.environ.get('SNSTOPICARN')
SNSSUBJECT = "Aws Options History Scraping Error Report"

# For now hard coding the exponiential moving avg error increase
#  that flags a SNS publication need to make a Config space Layer
EMAVGPUBLISHTHRESHOLDRATIO = 2.0

# Initialize the boto Clients and resources
logclient = boto3.client('logs')
ticker_table = boto3.resource('dynamodb').Table("OptionsHistTickers")
snsclient = boto3.client('sns')


class MyEncoder(json.JSONEncoder):
    """Simple JSONEncoder Extenstion to handle Decimal Types"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        return super(MyEncoder, self).default(obj)


class UnreachableReport:

    def __init__(self):
        self._html = self.html_template()

    def get_html(self):
        return self._html

    def publish_sns(self):
        snsclient.publish(
                TopicArn=SNSTOPICARN,
                Subject=SNSSUBJECT,
                Message=self._html
                )

    def html_template(self):
        return """
<html>
<body>
<h1>  NOT IMPLEMNTED YET </h1>
</body>
</html>
"""


def is_flagged(errors, item, run_timestamp):
    """
    Inputs:------------------------------------------------------------
    errors --> is a list of log events (expiration_date, exception string)
        associated with item['Ticker']

    item --> is the whole item that was returned from the ticker_table

    run_timestamp --> timestamp for when the collection was started

    OutPuts:-----------------------------------------------------------
    Tuple: (item, flag, reason)

    item (the updated DynamoDB item from inputs),
    flag (Boolean) is Flagged returns true to SNS publish,
    reason (String), the text that will show up in the SNS publication

    REMINDERS:
    This function is responsible for the processing of the last
    collect runs errors and for the update of the item, which will
    be updated in dynamodb after the handler function passes item
    into this function
    """

    # Check time stamp of last log processed timestamp to prevent
    #  double collection of data in testing and or set timestamp
    #  for initial collection of data
    ts = item.get("last_timestamp")
    if ts is None:
        item['last_timestamp'] = run_timestamp
    elif ts == run_timestamp:
        # this item was already updated with last run information
        return item.get("flag_state"), item.get("flag_reason"), item
    else:
        item['last_timestamp'] = run_timestamp

    # Check for "No Expiration Dates error always flag these errors"
    for expiration, event in errors:
        if re.match('No Expiration Dates', event):
            item["flag_state"] = True
            item["flag_reason"] = event
            return True, event, item

    # Adjust values for error collection
    errorlog = item.get("errors")
    if errorlog is None:
        item['errors'] = errors
        item['err_emavg'] = Decimal('{:.6f}'.format(len(errors) + 1e-5))
        item['flag_state'] = False
        item['flag_reason'] = None
        return False, None, item

    else:
        num_errors = len(errors)
        emavg = item['err_emavg']
        new_emavg = .25 * num_errors + .75 * float(emavg)
        item['errors'] += errors
        item['err_emavg'] = Decimal('{:.6f}'.format(new_emavg))
        if new_emavg / float(emavg) > EMAVGPUBLISHTHRESHOLDRATIO:
            item['flag_state'] = True
            item['flag_reason'] = "EMAVG THRESHOLD EXCEEDED"
            return True, "EMAVG THRESHOLD EXCEEDED", item
        else:
            item['flag_state'] = False
            item['flag_reason'] = None
            return False, None, item


def handler(event, context):
    # Get the log events of the last collect run
    parsedlogs = event.get('Records')
    if parsedlogs is None:
        logger.info("LambdaProxyFunction Invoked --")
        # If the function was invoked by API Gateway and not the
        #  the event source mapping generate a unreachable data report
        #  return HTML doc of the report and publish to SNSTOPICARN
        #  environment variable is set
        report = UnreachableReport()
        if SNSTOPICARN is not None:
            report.publish_sns()
        return {"html": report.get_html()}

    logger.info("SQS Invoked -- ")
    parsedlogs = json.loads(parsedlogs[0]['body'])  # SQS BatchSize=1
    run_timestamp = parsedlogs['run_timestamp']
    parsedlogs.pop('run_timestamp')

    # Loop over the collection table and check current Errors
    #  from baseline errors
    for ticker, errors in parsedlogs.items():
        # Get the tickers item from ticker_collection table
        item = ticker_table.get_item(
                Key={'Ticker': ticker, 'Collecting': 'TRUE'}
            ).get('Item')

        if item is None:
            # The item was removed from collection in the iterim
            #  continue on
            continue

        # Process the log events and up
        flag, reason, item = is_flagged(errors, item, run_timestamp)
        logger.info('{} -- flagged for -- {}'.format(ticker, reason))
        flatItem = json.dumps(item, cls=MyEncoder)
        while len(flatItem.encode()) > 4000:
            # Pop off error events try to keeping the items below
            # the 4KB mark
            item['errors'].pop(0)
            flatItem = json.dumps(item, cls=MyEncoder)
        # Update the item logs in the dynamoDB table.
        #  with current error logging metadata
        ticker_table.update_item(
            Key={"Ticker": ticker, "Collecting": "TRUE"},
            UpdateExpression='SET errors = :errors, last_timestamp = :ts, '
            + 'flag_reason = :reason, flag_state = :state, '
            + 'err_emavg = :eavg',
            ExpressionAttributeValues={
                ":errors": item.get('errors'),
                ":ts": item.get('last_timestamp'),
                ":reason": item.get('flag_reason'),
                ":state": item.get('flag_state'),
                ":eavg": item.get('err_emavg')
                },
            ConditionExpression='attribute_exists(Collecting)'
            )

    return 0
