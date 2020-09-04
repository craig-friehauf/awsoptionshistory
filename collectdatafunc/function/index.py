"""
AWS Lambda Function for OptionsHistory project
Name: OptionsHistory-CollectDataFunc

Inputs Events:
    -------------------------------------------------------------------
    {"State": "initialize"} --> Get the Collection List and start
    {
        "State": "continue",
        "Tickers": ["AAPL",...]
        "runs_timestamp": UnixTimeStamp from initial call
    } --> continue will collection run

    State must be defined or function will fail
    If State is continue and Tickers doesn't exist fail

OutPut:
    --------------------------------------------------------------------
    Takes all tickers that have parition key "Collect" == "TRUE" in
    OptionsHistTickerTable and attempts to collect options data from
    finance.yahoo.com and stores it in DynamoDB

File OverView:
    -------------------------------------------------------------------

    Funtions that start with _ are helper functions. There are 6 main
        functions that work together to build a 5 stage asyncio pipeline
        to collect, tranform and store options data

    async def async_handler: constructs all async tasks and starts the
        pipline

    Pipeline functions:
        Stage 1: async def ticker_handler
            Fills the queue between Stage 1 and 2 then passed remaining
            tickers to next invocation of the collection function
        Stage 2: async def get_expiration_dates
            Get the exipration dates for a ticker and stage the values
            for stage 3
        Stage 3: async def chain_request
            Makes http requests to get the data for a (ticker, expiration)
        Stage 4: async def encode_db_item
            Transform the data in to a JSON document for insertion in
            DynamoDB
        Stage 5: async def put_db_item
            Makes put calls to DynamoDB to store the data
"""


import base64
import boto3
import json
import os
import asyncio
import aiohttp
import numpy as np
import pandas as pd
from datetime import datetime
import time
from boto3.dynamodb.conditions import Key
import logging


if os.environ.get("XRAYACTIVATED") is not None:
    # Xray Has been activated for the stack, patch calls to AWS services
    #  with X-Ray Trace Headers and send trace segments to X-Ray deamon
    # from aws_xray_sdk.core import xray_recorder currently not using
    from aws_xray_sdk.core import patch_all
    # Patch for X-Ray Tracing to DynamoDB and AWS API calls
    patch_all()

# Initialize logging module for the lambda function
logger = logging.getLogger()
logger.setLevel(getattr(logging, os.environ['LOGLEVEL']))

# Played around, these seem to work the best to saturate the Lambda's
# ENI and stays with in current memory setting for this function as well
# keeps the write capacity units (WRUs) for DynamoDB under 8
TICKERQUEUESIZE = int(os.environ['TICKERQUEUESIZE'])
MAXCONNECTIONS = int(os.environ['MAXCONNECTIONS'])

# Initialize AWS service clients and resources
table = boto3.resource('dynamodb').Table("OptionsHist")
ticker_table = boto3.resource('dynamodb').Table("OptionsHistTickers")
lambdaclient = boto3.client('lambda')
sqsqueue = boto3.resource('sqs').Queue(os.environ['NOTREACHABLESQS'])

# USED to collect all the unreachable messages to be sent to SQS
#  for update of the OptionsHistTicker Table
UNREACHABLEMESSAGES = None

# Stage shutdown globals these should all be wrapped in a class I think
#  this will work for now give more freedom on picking stage concurrency
stage1shutdown = None
stage2shutdown = None
stage3shutdown = None
stage4shutdown = None
stage5shutdown = None


def _unreachable_message(ticker, expiration_date, e):
    """
    Construct Invocations Unreachable SQS Message
    """
    global UNREACHABLEMESSAGES
    if UNREACHABLEMESSAGES is None:
        # Debug Error checking
        raise Exception("UNREACHABLEMESSAGES is None")

    try:
        UNREACHABLEMESSAGES[ticker].append((expiration_date, str(e)))
    except KeyError:
        UNREACHABLEMESSAGES[ticker] = [[expiration_date, str(e)]]


def _build_options_url(ticker, date=None):
    """
    Help Function
    Constructs the URL pointing directly to the options chain for a
     ticker and optional date

    Inputs:
        ---------------------------------------------------------------
        ticker --> (string) i.e. "AAPL"
        date --> (string) "Month DD, YYYY" i.e. May 15, 2020

    Caveats: Future
        ---------------------------------------------------------------
        1.) This is url format that currently works on a majority of
            tickers for yahoo finance. NOT future proof in way.

    """

    url = "https://finance.yahoo.com/quote/" + ticker + "/options?p=" + ticker
    if date is not None:
        url = url + "&date=" + str(int(pd.Timestamp(date).timestamp()))

    return url


def _encodeOptionsTable(optstab):
    """
    Helper Function
    Inputs:
        ---------------------------------------------------------------
        optstab --> (pd.DataFrame) Generated from pandas.read_html()

    Output:
        ---------------------------------------------------------------
        (JSON) -->
            {
                "Last Trade Date" --> (list[(strings)])
                "Column Labels" --> (list[(strings)])
                "Shape" --> base64 encoded np.array(dtype=np.int32)
                "Table" --> base64 encoded np.array(dtype=np.float16).flatten()
            }

    Caveats: Future
        ---------------------------------------------------------------
        1.) This is also highly coupled to everything else in this file
            not future proof

    Tansforms the pandas.DataFrame generated by pandas.read_html(htmlsource)
    for storage in DynamoDB.

    Coverts to half-percision floats and base64 encodes the np.array,
    sacrifices a little percision for reduced storage.
    """

    def _string2Number(string):
        """
        Helper Function
        Coverts the strings in pandas Dataframe to
        floats.
        """

        if isinstance(string, str):
            # Repalce Commas
            if ',' in string:
                # Pull all the commas out if they exist
                string = string.replace(',', '')

            if len(string) == 0 or string == "-":
                return np.nan
            if string[-1] == '%':
                string = string[:-1]

        return float(string)

    # If Options table is None return None
    if optstab is None:
        return optstab

    # Drop redundent columns and hold on to string type columns
    record = {}
    optstab = optstab.drop(['Contract Name'], axis=1)
    record['Last Trade Date'] = list(optstab['Last Trade Date'])
    optstab = optstab.drop(['Last Trade Date'], axis=1)
    record['Column Labels'] = list(optstab.keys())
    optstab = optstab.to_numpy()

    # Base64 encode the shape of the array for reconstruction
    record['Shape'] = base64.b64encode(
        np.array(optstab.shape, dtype=np.int32).tobytes()
        ).decode('ascii')

    # Map sting values to numbers, convert to half percision numbers
    #  and base64 incode the array
    record['Table'] = base64.b64encode(
            np.array(
                list(map(_string2Number, optstab.flatten())),
                dtype=np.float16
                ).tobytes()
            ).decode('ascii')

    return json.dumps(record)

#######################################################################
# Main Async functions that implements the stages of the collection
# pipeline


async def ticker_handler(tickers, context, queue_out, HTTPSession):
    """
    STAGE 1
    Inputs:
        ----------------------------------------------------------------
        tickers --> (list) tickers that still need to be collected
        context --> (lambda context) the context object passed to the
            handler
        queue_out --> (asynio.Queue) Queue that stages
            (ticker, expiration dates) for the next stage in the pipeline
        HTTPSession --> (aiohttp.ClientSession) shared object for making
            http request across all stages

    Reminders:
        ----------------------------------------------------------------
        STAGE 1: of the async pipline called as a coroutine from
            async_handler. Single instance of this function stages
            (ticker, expiration date) for chain_request(Stage 2).
    """
    global stage1shutdown

    # Process the tickers and stage them for stage 2
    while len(tickers) > 0:

        if (context.get_remaining_time_in_millis() < 280000 and
                len(tickers) > 0):
            # running out of time and there are still tickers
            #  invoke another lambda function and initiate shutdown of
            #  the other stages
            logger.info(
                "Time remaining: {}, Number of Tickers Left: {}".format(
                    context.get_remaining_time_in_millis(), len(tickers))
                )
            lambdaclient.invoke(
                FunctionName='OptionsHistory-CollectDataFunc',
                InvocationType='Event',
                Payload=json.dumps(
                    {
                        'State': 'continue',
                        'Tickers': tickers,
                        'run_timestamp': UNREACHABLEMESSAGES['run_timestamp']
                    }
                    ).encode()
                )

            break
        elif len(tickers) == 0:
            break
        else:
            ticker = tickers.pop()
            await queue_out.put(ticker)
            logger.debug(
                        "QSIZE STAGE1 -> STAGE2 -- {}".format(
                            queue_out.qsize()
                            )
                        )

    # Signal stage 2 that no more values are coming finish up and shutdown
    stage1shutdown = True
    await queue_out.put(None)
    logger.debug("STAGE1 RETURNING")
    return None


async def get_expiration_dates(queue_in, queue_out, HTTPSession):
    """
    Stage 2
    Input:
        -----------------------------------------------------------
        ticker --> (String) i.e. "AAPL"

    Output:
        -----------------------------------------------------------
        (list) of expiration dates for options contracts in format
            "Month DD, YYYY" i.e. May 15, 2020
    """
    global stage2shutdown

    while True:

        ticker = await queue_in.get()
        logger.debug(
                    "QSIZE STAGE1 -> STAGE2 -- {}".format(
                        queue_in.qsize()
                        )
                    )
        if ticker is None:
            # Run the concurrent task shutdown process
            queue_in.task_done()
            await queue_in.join()
            if stage2shutdown is False:
                await queue_out.put((None, None))
                stage2shutdown = True
            await queue_in.put(None)
            logger.debug("STAGE2 RETURNING")
            return None

        # Get base URL for the ticker
        url = _build_options_url(ticker)
        async with HTTPSession.get(url) as response:
            html = await response.text()

        # Find the option elements for the drop down menu
        # parse into a list of dates
        splits = html.split("</option>")
        dates = [elt[elt.rfind(">"):].strip(">") for elt in splits]
        dates = [elt for elt in dates if elt != '']

        if len(dates) == 0:
            # If no expiration dates can be found log, and continue
            _unreachable_message(
                ticker, "NONE", Exception("No Expiration Dates")
                )
            queue_in.task_done()
            continue

        else:
            for expire in dates:
                await queue_out.put((ticker, expire))
                logger.debug(
                    "QSIZE STAGE2 -> STAGE3 -- {}".format(
                        queue_out.qsize()
                        )
                    )
            queue_in.task_done()


async def chain_request(queue_in, queue_out, HTTPSession):
    """
    STAGE 3
    Inputs:
        ----------------------------------------------------------------
        queue_in --> (asyncio.Queue) Queue between Stage 1 and this
        queue_out --> (asyncio.Queue) Queue between this and Stage 3
        HTTPSession --> (aiohttp.ClientSession) shared object for making
            http request across all stages

    Reminders:
        ----------------------------------------------------------------
        Stage 2: of the aync pipeline. Called as coroutine from
            asnyc_handler. Multiple instances of this function run
            simultaneously
    """
    global stage3shutdown

    while True:
        # Block until the next (ticker, expiration_date) is queued
        ticker, expiration_date = await queue_in.get()
        logger.debug(
                    "QSIZE STAGE2 -> STAGE3 -- {}".format(
                        queue_in.qsize()
                        )
                    )
        if ticker is None:
            # Wait to make sure curcurrent tasks finish before signaling
            #  stage 3 and other concurrent tasks to shutdown
            queue_in.task_done()
            await queue_in.join()
            if stage3shutdown is False:
                await queue_out.put((None, None, None))
                logger.debug("PUT2STAGE4")
                stage3shutdown = True
            await queue_in.put((None, None))
            logger.debug("STAGE3 RETURNING")
            return None

        else:
            # try rap to collect and log errors with out failure
            try:
                # Make the intial HTTP request more elegent solution
                #  is need here
                url = _build_options_url(ticker, expiration_date)
                async with HTTPSession.get(url) as response:
                    html = await response.text()

                tables = pd.read_html(html)
                data = {}

                try:
                    data['calls'] = tables[0]
                except IndexError:
                    _unreachable_message(
                        ticker, expiration_date, Exception("No Calls Data")
                        )
                    data['calls'] = None

                try:
                    data['puts'] = tables[1]
                except IndexError:
                    _unreachable_message(
                        ticker, expiration_date, Exception("No Puts Data")
                        )
                    data['puts'] = None

                if not (data['puts'] is None and data['calls'] is None):
                    # If there is data push to encode_db_item stage
                    await queue_out.put((ticker, expiration_date, data))
                    logger.debug(
                        "QSIZE STAGE3 -> STAGE4 -- {}".format(
                            queue_out.qsize())
                        )
                else:
                    # If no data was found log event and move to next item
                    _unreachable_message(
                        ticker, expiration_date, Exception("No Data")
                        )

            except Exception as e:
                _unreachable_message(ticker, expiration_date, e)

            # Signal the queue task is complete for this (ticker, expiration)
            queue_in.task_done()


async def encode_db_item(queue_in, queue_out):
    """
    STAGE 4
    Inputs:
        ----------------------------------------------------------------
        queue_in --> (asyncio.Queue) Queue between Stage 2 and this
        queue_out --> (asyncio.Queue) Queue between this and Stage 4

    Reminders:
        ----------------------------------------------------------------
        Stage 4: of the aync pipeline. There is really no need for more
        then one of this task to run. no blocking IO done here and I
        beleive at the moment the lambda enviroment is a single core
    """

    global stage4shutdown

    while True:
        # Block until the next options_chain is available
        ticker, expiration_date, data = await queue_in.get()
        logger.debug(
            "QSIZE STAGE3 -> STAGE4 -- {}".format(queue_in.qsize())
            )

        if ticker is None:
            # If (None, None, None) comes through the queue exit function
            queue_in.task_done()
            await queue_in.join()
            if stage4shutdown is False:
                await queue_out.put(None)
                stage4shutdown = True
            await queue_in.put((None, None, None))
            logger.debug("STAGE4 RETURNING")
            return None

        else:
            item = {}
            # Get current collection time
            _time = datetime.now().strftime('%Y%m%d%H%M%S')
            # Set field for DynamoDB Partion Key
            item['Ticker'] = ticker
            # Set field for DynamoDB Sort Key
            item['TimeCollectedExpirationDate'] = int(
                _time + datetime.strptime(
                    expiration_date, "%B %d, %Y"
                    ).strftime('%Y%m%d')
                )
            try:
                item['calls'] = _encodeOptionsTable(data['calls'])
                item['puts'] = _encodeOptionsTable(data['puts'])
                await queue_out.put(item)
                logger.debug(
                    "QSIZE STAGE4 -> STAGE5 -- {}".format(queue_out.qsize())
                    )
            except Exception as e:
                # if any thing goes wrong with encoding the items
                #  log the error
                _unreachable_message(ticker, expiration_date, e)
            queue_in.task_done()


async def put_db_item(queue_in):
    """
    STAGE 5
    Async function to put the items into the Dynamdb Table collections
    as many that are current

    Inputs:
        ----------------------------------------------------------------
        queue_in --> (asyncio.Queue) Queue between Stage 3 and this
            queue.pop() --> (JSON) item to put to dynamodb

    """

    global stage5shutdown

    while True:

        if stage5shutdown is True:
            await queue_in.join()
            await queue_in.put(None)
            logger.debug("STAGE5 RETURNING")
            return None
        else:
            items = []

        try:
            # Get All Items that are ready to put to dynamodb
            while True:
                items.append(queue_in.get_nowait())
                logger.debug(
                    "QSIZE STAGE4 -> STAGE5 -- {}".format(queue_in.qsize())
                    )
        except asyncio.QueueEmpty:
            if len(items) == 0:
                # Block until an item is available
                items.append(await queue_in.get())
                logger.debug(
                    "QSIZE STAGE4 -> STAGE5 -- {}".format(queue_in.qsize())
                    )
        if items[-1] is None:
            stage5shutdown = True
            # Pop the none off the list of items
            items.pop()
            queue_in.task_done()

        if len(items) > 0:
            try:
                with table.batch_writer() as batch:
                    for item in items:
                        batch.put_item(Item=item)
                        queue_in.task_done()
            except Exception as e:
                _unreachable_message("DYNAMODB", "NONE", e)


async def async_handler(tickers, context):
    """
    Entry point into the async event loop

    Inputs:
        ----------------------------------------------------------------
        tickers --> list[strings]
        context --> Lambda invokation context object
    Reminders:
        ----------------------------------------------------------------
        Be careful with the max queue sizes the shutdown process has to
        be accounted for.

    """

    # Initialize Queue between a ticker_handler and get_expiration_dates
    th2ge = asyncio.Queue(maxsize=TICKERQUEUESIZE)
    # Initiailize Queue betweem get_expiration_dates and chain_request
    ge2cr = asyncio.Queue(maxsize=TICKERQUEUESIZE + MAXCONNECTIONS)
    # Initialize the queue between chain_request and encode_db_item
    cr2edi = asyncio.Queue(maxsize=TICKERQUEUESIZE + MAXCONNECTIONS)
    # Initiaize Queue between encode_db_item and put_db_item
    edi2pdi = asyncio.Queue(maxsize=TICKERQUEUESIZE + MAXCONNECTIONS)

    # initalize an aiohttp Client Session and masqurade as a
    #  firefox client
    HTTPSession = aiohttp.ClientSession(headers={
        "user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:75.0)"
        + " Gecko/20100101 Firefox/75.0",
        "Accept": "text/html",
        "Accept-Encoding": "identity"
        })

    # Create a List of Coroutines to run concurrently as tasks
    # Stage 1: ticker_handler.
    tasks = [ticker_handler(tickers, context, th2ge, HTTPSession)]

    for i in range(TICKERQUEUESIZE):
        tasks.append(get_expiration_dates(th2ge, ge2cr, HTTPSession))

    # Stage 2: Many Stage. chain_requests to run concurrently
    for i in range(MAXCONNECTIONS):
        tasks.append(chain_request(ge2cr, cr2edi, HTTPSession))

    # Stage 3: encode_db_item, No blocking io so single coroutine
    tasks.append(encode_db_item(cr2edi, edi2pdi))

    # Stage 4: Many stage. Put items to the DynamoDB as block io
    for i in range(2):
        tasks.append(put_db_item(edi2pdi))

    # Launch all the coroutines as tasks and wait for them to complete
    await asyncio.gather(*tasks)
    # Close HTTPSession
    await HTTPSession.close()
    # End async loop and Give control back to the lambda handler
    return None


def handler(event, context):
    """
    Main handler function for AWS Lambda Invocation
    See head of file for on valid input events
    """

    # Initilaize invocations UNREACHABLE data mapping and
    #  stage shutdown globals
    global UNREACHABLEMESSAGES
    global stage1shutdown, stage2shutdown, stage3shutdown
    global stage4shutdown, stage5shutdown
    UNREACHABLEMESSAGES = {"run_timestamp": time.time_ns()}
    stage1shutdown = False
    stage2shutdown = False
    stage3shutdown = False
    stage4shutdown = False
    stage5shutdown = False

    state = event['State']
    # Determine what to do based on event state
    if state == "initialize":
        # Logging the initalization of collection process
        #  log processing function needs this log event
        #  so it knows how far back in to process the logs
        logger.info("State: initialize, run_timestamp: {}".format(
            UNREACHABLEMESSAGES['run_timestamp'])
            )

        tickers = ticker_table.query(
            KeyConditionExpression=Key('Collecting').eq("TRUE"),
            ProjectionExpression='Ticker'
            ).get('Items')

        if tickers is None:
            # Was unable to get collection list from dynamoDB log error
            #  and return
            _unreachable_message(
                "NONE", "NONE", Exception("""
                    Was Unable to collect tickers from OptionHistTickers
                    DynamoDB Table. Have you added any tickers to the
                    collection list using the API
                    """)
                )
        else:
            # construct ticker list from list of return DynamoDB items
            tickers = list(map(lambda x: x.get("Ticker"), tickers))
            logger.info(
                "Initialize Number of Tickers: {}".format(len(tickers))
                )
            asyncio.run(async_handler(tickers, context))

    elif state == "continue":
        # Continue Collection with the tickers left in the list

        tickers = event['Tickers']
        UNREACHABLEMESSAGES["run_timestamp"] = event["run_timestamp"]
        logger.info(
            "State: continue, Number of remaining tickers: {}".format(
                len(tickers))
            )
        # Initilize the async event loop and launch the aysnc_handler
        asyncio.run(async_handler(tickers, context))

    else:
        # Invalid state event invoked the function log and return
        _unreachable_message("NONE", "NONE", Exception("""
            OptionsHistory-CollectDataFunc -- received in invalid event[State]
            """ + str(event['State'])))

    logger.info("Exiting Invocation, Runs total time mins. -- {}".format(
        1e-9 * (
            time.time_ns() - UNREACHABLEMESSAGES['run_timestamp']
            ) / 60.)
        )
    # Send the unreachable data to the SQS
    sqsqueue.send_message(
            MessageBody=json.dumps(UNREACHABLEMESSAGES)
        )
    logger.info("Send sqs message")

    return 0
