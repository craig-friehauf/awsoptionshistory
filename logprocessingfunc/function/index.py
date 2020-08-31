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

if not os.environ.get("XRAYACTIVATED") is None:
    # Xray Has been activated for the stack, patch calls to AWS services
    #  with X-Ray Trace Headers and send trace segments to X-Ray deamon
    #  from aws_xray_sdk.core import xray_recorder not currently used
    from aws_xray_sdk.core import patch_all
    # Patch for X-Ray Tracing to AWS Service API calls
    patch_all()

# The log group from collectdatafunc right now I am gonna hard code
#  this in here for should figure out a better way to do this
LOGGROUP = os.environ["COLLECTIONLOGGROUP"]
# Pull the SNS TOPIC ARN in from the enviorment
SNSTOPICARN = os.environ.get('SNSTOPICARN')
SNSSUBJECT = "Aws Options History Scraping Error Report"

# For now hard coding the exponiential moving avg error increase
#  that flags a SNS publication need to make a Config space Layer
EMAVGPUBLISHTHRESHOLDRATIO = 2.0
# Publish No Expiration Dates for a ticker to the SNS Topic
PUBLISHNOEXPIRATIONDATES = True

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


class SNSMessageBuilder:
    """
    Simple Class for holding come code to generate message strings
    for SNS publishing errors
    """

    def __init__(self, publish_errors):
        self._pes = publish_errors

    def get_text(self):
        message = ''
        for ticker, reason in self._pes:
            message += "{} : reason --> {}\n".format(ticker, reason)
        return message

    def get_html(self):
        return 'NOT IMPLEMENTED YET'


def parseLogs():
    """
    INPUTS:---N/A
    OUTPUTS:-- {ticker: [(expiration_date, error)]}
    Function that runs through all the log events for a given Collection
    run orgainizes tham as a mapping of tickers to a "list of tuples"
    (expiration_date, exception string)

    REMINDERS: See initial notes about "--Init Collect Data"
    """

    parsedLogs = {}

    # Get the log streams ordered backward in time from now
    # I beleive at the moment getting the
    streams = logclient.describe_log_streams(
            logGroupName=LOGGROUP,
            orderBy='LastEventTime',
            descending=True,
            limit=30
        )
    # Notes on response and to oneself
    # DECENDING SO the last event in each stream happened later in time
    # i.e. Once --Init Collect Data is found keep processing
    # streams until a streams lastEventTimeStamp is before the
    # --Init Collect Data Event. At this point I am gonna assume
    # limit of 30 is plenty even with a full SEC list of tickers
    # I beleive at this point this might have something to do
    # concurrent executes. Given there is two at most. REMINDER
    # given a future attempt more consitant time interval collection
    # {
    #     'logStreams': [
    #         {
    #             'logStreamName': 'string',
    #             'creationTime': 123,
    #             'firstEventTimestamp': 123,
    #             'lastEventTimestamp': 123,
    #             'lastIngestionTime': 123,
    #             'uploadSequenceToken': 'string',
    #             'arn': 'string',
    #             'storedBytes': 123
    #         },
    #     ],
    #     'nextToken': 'string'
    # }

    inittime = None

    for stream in streams.get('logStreams'):
        # Check if inittime is before the streams last event if it is
        #  then the stream has no event releavant to the last collect
        #  run
        if inittime is None:
            pass
        elif inittime > stream['lastEventTimestamp']:
            # all events in last run have been accounted for
            break

        nextToken = None
        # Get the log events from the steam
        while True:
            # Logic for handling large streams if not all events in the
            #  the stream are over 1MB which is the max return size for
            #  CloudWatch Logs API set the next token for the next loop
            if nextToken is None:
                events = logclient.get_log_events(
                        logGroupName=LOGGROUP,
                        logStreamName=stream.get('logStreamName'),
                        startFromHead=False
                    )
                nextToken = events.get('nextBackwardToken')
            else:
                events = logclient.get_log_events(
                        logGroupName=LOGGROUP,
                        logStreamName=stream.get('logStreamName'),
                        nextToken=nextToken
                    )
                nextNextToken = events.get('nextBackwardToken')
                # break the while loop if the API call returns
                # the same nextToken (i.e. no more events in this stream)
                if nextNextToken == nextToken:
                    break
                else:
                    nextToken = nextNextToken
            # Notes on events object
            # {
            #     'events': [
            #         {
            #             'timestamp': 123,
            #             'message': 'string',
            #             'ingestionTime': 123
            #         },
            #     ],
            #     'nextForwardToken': 'string',
            #     'nextBackwardToken': 'string'
            # }

            # Reverse the order of the events in the list
            #  given the parameters in the API call this will run
            #  over the Log events in this stream from future --> past
            for event in reversed(events.get('events')):
                # Do to the asyrounous nature of the rest of the
                #  collectdatafunc check to see if the init event
                #  has been hit and if the event is before it acccured
                if inittime is None:
                    pass
                elif inittime > stream['lastEventTimestamp']:
                    # all events in last run have been accounted for
                    break

                message = event['message']
                if re.match('--Init Collect Data', message):
                    # reach the end of the current runs logged events
                    inittime = event['timestamp']

                match = re.match('\\[(.*)\\] \\[(.*)\\] \\[(.*)\\]', message)
                if match is None:
                    # Log Event was not a scraping error exception
                    continue
                else:
                    ticker = match.group(1)
                    exipration_date = match.group(2)
                    error = match.group(3)
                    try:
                        parsedLogs[ticker].append([exipration_date, error])
                    except KeyError:
                        parsedLogs[ticker] = [[exipration_date, error]]

    return inittime, parsedLogs


def is_flagged(errors, item, run_timestamp):
    """
    Inputs:------------------------------------------------------------
    errors --> is a list of log events (expiration_date, exception string)
        associated with item['Ticker']

    item --> is the whole item that was returned from the ticker_table

    run_timestamp --> timestamp of when --Init Collect Data log event
        accured.

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
            # Check to see if no expirationdates is publishable error
            if PUBLISHNOEXPIRATIONDATES is True:
                item["flag_state"] = True
            else:
                item["flag_state"] = False

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
    run_timestamp, parsedlogs = parseLogs()
    publish = False

    # Check for CRITICAL failures
    if 'NONE' in parsedlogs.keys():
        # There was a failure in getting tickers that are being collected
        #  publish CRITICAL ERROR to SNS Topic
        publish = True
        SNSMessage = """
Critical NONE was found. Check CollectDataFunction Logs Associated with
issues getting ticker list from OptionsHistTicker DynamoDB
"""
    elif 'DYNAMODB' in parsedlogs.keys():
        # There was a  item error in collection publish as CRITICAL
        #  ERROR to SNS Topic
        publish = True
        SNSMessage = """
Critical DYNAMODB was found. Check CollectDataFunction Logs
        """
    else:

        # These always get published to the SNS topic for investigation
        #  this means were getting no data if it can't be fixed from
        #  the ticker from collect
        errors2SNS = []

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
            if flag is True:
                errors2SNS.append((item['Ticker'], reason))

            flatItem = json.dumps(item, cls=MyEncoder)
            while len(flatItem.encode()) > 4000:
                # Pop off error events try to keep the items below
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
        # Publish to SNS with the report for human intervention if there
        # is anything to report.
        if len(errors2SNS) > 0:
            publish = True
            SNSMessage = SNSMessageBuilder(errors2SNS).get_text()

    if publish is True and not (SNSTOPICARN is None):
        snsclient.publish(
                TopicArn=SNSTOPICARN,
                Subject=SNSSUBJECT,
                Message=SNSMessage
                )

    return 0
