"""
ApiGateway Simple Lambda Proxy

Using a wrapper to build a simple WSGI application using
the wekzeug Tools Set
pip install werkzeug
pip install awsgi (I had to modifiy this to get it to work)

API Endpoints
------------------------------------------------------------------------

GET /data?Ticker=AAPL&day=YYYY-MM-DD -->
    pandas.read_json(HTTPresponse).set_index(['index']) -->
    pandas.DataFrame() --> All options data collected that day

GET /data/encoded?Ticker=AAPL&YYYY-MM-DD -->
    client side decoding of the DynamoDB stored object the pandas
    DataFrame loading like above see clientexample.py for example

GET /tickers --> JSON({"Tickers": [...]}) get the current tickers
    that collection runs are currently scraping data for as well as
    collected intervals

POST /tickers?Ticker=AAPL --> Add a companies ticker that will be collected
    on each collection run

DELETE /tickers?Ticker=AAPL --> Remove a ticker from the current collection
    runs

POST /collect --> States a collection run gathering options data for current
    tickers set to be collected

POST /purge?Ticker=AAPL|uuid='uuid' --> delete all collected data on the
    security. __NOT CURRENTLY IMPLEMENTED

REMINDERS
------------------------------------------------------------------------
Also see CRONSCHEDULE in deploy-stack.sh for setting up data collect
on a schedule

from werkzeug.exceptions import NotFound --> Exception thats used for
    route to not found in Map
"""

import boto3
from boto3.dynamodb.conditions import Key
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
import awsgi
import json
import os
import base64
import pandas as pd
import numpy as np
from datetime import datetime
from decimal import Decimal
import re
import uuid

if not os.environ.get("XRAYACTIVATED") is None:
    # Xray Has been activated for the stack, patch calls to AWS services
    #  with X-Ray Trace Headers and send trace segments to X-Ray deamon
    # from aws_xray_sdk.core import xray_recorder (Currently not used)
    from aws_xray_sdk.core import patch_all
    # Patch for X-Ray Tracing with DynamoDB
    patch_all()


def _decodeOptionsTable(record):
    """
    Helper Function. Transform the Base64 encoded table into Json object
    that can be loaded through pandas.read_json()
    Inputs:
    --------------------------------------------------------------------
    record --> json string that is stored in DynamoDB as attributes
        'calls' or 'puts'

    Outputs:
    --------------------------------------------------------------------
    dict, int --> python dictionary with keys for the columns of the
        table value sequence of values
    """

    def _last_trade_date_transformation(string):
        """
        Helper Function put last trade date in proper format
        Don actualy use here but keeping for now for python datetime
        transform with a pandas.DataFrame
        """
        return datetime.strptime(string[:-4], "%Y-%m-%d %I:%M%p")

    if record is None:
        # Calls and Puts table didn't have any data
        return None, None
    record = json.loads(record)
    frame = {"Last Trade Date": record["Last Trade Date"]}
    shape = np.frombuffer(
        base64.b64decode(record['Shape'].encode('ascii')),
        dtype=np.int32).astype(int)

    table = np.frombuffer(base64.b64decode(
        record['Table'].encode('ascii')), dtype=np.float16).reshape(shape)

    for label, data in zip(record['Column Labels'], table.T):
        frame[label] = data

    return frame, len(table)


def _decodeItem(item):
    """
    Decodes the dynamodb item into a tuple
    ( Ticker, str(expirationdate), datetime(Time Collected),
    calls(pd.DataFrame), puts(pd.DataFrame) )
    """

    ticker = item['Ticker']
    sortkey = str(item['TimeCollectedExpirationDate'])
    expiration = datetime.strptime(
        sortkey[-8:], '%Y%m%d').isoformat()[:10]
    collected = datetime.strptime(
        sortkey[:-8], '%Y%m%d%H%M%S').isoformat(" ")[:19]
    frame = []

    c_frame, table_len = _decodeOptionsTable(item['calls'])
    if c_frame is not None:
        c_frame["ticker"] = [ticker] * table_len
        c_frame["expirationdate"] = [expiration] * table_len
        c_frame["collectiontime"] = [collected] * table_len
        c_frame["contracttype"] = ['call'] * table_len
        frame.append(pd.DataFrame(c_frame))

    p_frame, table_len = _decodeOptionsTable(item['puts'])
    if p_frame is not None:
        p_frame["ticker"] = [ticker] * table_len
        p_frame["expirationdate"] = [expiration] * table_len
        p_frame["collectiontime"] = [collected] * table_len
        p_frame["contracttype"] = ['put'] * table_len
        frame.append(pd.DataFrame(p_frame))

    if len(frame) > 0:
        return pd.concat(frame)
    else:
        return None


class MyEncoder(json.JSONEncoder):
    """
    Simple JSONEncoder Extenstion to handle Decimal Types
    from DynamoDB
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        return super(MyEncoder, self).default(obj)


class WSGIApp(object):
    """
    Callable Class buildt as simple WSGI Application
    """
    def __init__(self):
        super(WSGIApp, self).__init__()
        # Route Map Object
        self.map = Map([
            Rule("/data", methods=['GET'], endpoint='jsonencode'),
            Rule("/data/encoded", methods=['GET'], endpoint='dbquery'),
            Rule("/tickers", methods=['POST'], endpoint='addticker'),
            Rule("/tickers",  methods=['GET'], endpoint='getticker'),
            Rule("/tickers", methods=['DELETE'], endpoint='rmticker'),
            Rule("/collect", methods=['POST'], endpoint='initializecollection')
            ])

    def _request_logging(self, endpoint, ip_address, info):
        """
        Helper function cloudwatch log requests in consistent structure
        """
        print("REQUEST [{}] [{}] [{}]".format(endpoint, ip_address, info))

    def _tickerParameterValidation(self, string):
        if string is None:
            return None
        if re.fullmatch("[A-Z.]+", string) is None:
            return None
        if len(string) > 8:
            return None
        return string

    def _dayparameterValidation(self, string):
        if string is None:
            return None
        if len(string) != 10:
            return None
        if re.fullmatch("\\d\\d\\d\\d-\\d\\d-\\d\\d", string) is None:
            return None
        return string

    def _dbquery(self, ticker, day):
        """
        Helper function used to make DynamoDB query for a tickers
        items collected on that day
        """
        begin = int(day + '0' * 14)
        end = int(day + '9' * 14)
        dat = dbtable.query(
             KeyConditionExpression=Key("Ticker").eq(ticker) &
             Key("TimeCollectedExpirationDate").between(begin, end)
            )
        return dat.get("Items")

    def getticker(self, environ, start_response):
        """
        GET /tickers

        Target for returning a json object []
        with all tickers that have collected data in the DynamoDB
        """

        tickers = tracked_tickers.scan(
            ProjectionExpression="Ticker, Collecting, Starting, Ending"
            ).get("Items")

        if len(tickers) == 0:
            self._request_logging(
                'getticker',
                str(environ.get('awsgi.requester')),
                'No tickers in the Collection List Yet'
                )
            return Response(
                "No Companies have been added to the Collection List\n"
                )(environ, start_response)

        response = Response(json.dumps(tickers))
        response.content_type = 'application/json'
        self._request_logging(
            'getticker',
            str(environ.get('awsgi.requester')),
            'success'
            )
        return response(environ, start_response)

    def addticker(self, environ, start_response):
        """
        POST /tickers

        Add a company/ticker to the Collection list
        """
        request = Request(environ)
        Ticker = self._tickerParameterValidation(
            request.args.get('Ticker')
            )
        if Ticker is None:
            self._request_logging(
                'addticker',
                str(environ.get('awsgi.requester')),
                'Bad Ticker parameter must match [A-Z.]+'
                )
            return Response(
                "Bad Request parameters see README.md\n"
                )(environ, start_response)

        tracked_tickers.update_item(
                Key={
                    "Ticker": Ticker,
                    "Collecting": "TRUE"
                }
            )

        self._request_logging(
            'addticker',
            str(environ.get('awsgi.requester')),
            'Added ticker to Collection List {}'.format(Ticker)
            )
        return Response(
            "Ticker {} added to the collection list.\n".format(Ticker)
            )(environ, start_response)

    def rmticker(self, environ, start_response):
        """
        DELETE /tickers
        remove company/ticker from the collection list
        """

        request = Request(environ)
        Ticker = self._tickerParameterValidation(
            request.args.get('Ticker')
            )
        if Ticker is None:
            self._request_logging(
                'rmticker',
                str(environ.get('awsgi.requester')),
                'Bad Ticker parameter must match [A-Z.]+'
                )
            return Response(
                "Bad Request see README.md Ticker=[A-Z.+]\n"
                )(environ, start_response)

        # Check for a current db item
        item = tracked_tickers.get_item(
                Key={
                    "Ticker": Ticker,
                    "Collecting": "TRUE"
                }
            ).get('Item')
        # Need to debug this might be empty list
        if item is not None:
            # remove the item from the tracked_ticker table
            tracked_tickers.delete_item(
                    Key={
                        "Ticker": Ticker,
                        "Collecting": "TRUE"
                    }
                )
            # Check to see if any day was actually collect and if so
            #  reinsert the item for a record of interval data
            if not item.get('Starting') is None:
                # Data was collect put new item with Collecting attribute
                # with todays date
                item['Collecting'] = str(uuid.uuid4())
                # Remove the Errors list for smaller foot print
                try:
                    item.pop('errors')
                except KeyError:
                    pass

                tracked_tickers.put_item(Item=item)

        self._request_logging(
                'rmticker',
                str(environ.get('awsgi.requester')),
                'Removed Ticker {} from collecting list'.format(Ticker)
                )

        return Response(
            "Ticker {} Removed from Collection List\n".format(Ticker)
            )(environ, start_response)

    def dbquery(self, environ, start_response):
        """
        GET /data/encoded

        Returns JSON encoded in same way it is stored in dynamoDB
        requires client side decoding to get back a consitent pandas
        DataFrame see clientexample/clientexample.py
        """

        request = Request(environ)
        Ticker = self._tickerParameterValidation(
            request.args.get('Ticker')
            )
        day = self._dayparameterValidation(
            request.args.get('day')
            )

        if Ticker is None or day is None:
            self._request_logging(
                'dbquery',
                str(environ.get('awsgi.requester')),
                'Bad Request Parameters')
            return Response(
                "Bad Request -- see README.md"
                )(environ, start_response)

        day = day.replace("-", "")
        items = self._dbquery(Ticker, day)
        # Single day collection data should never come close to the
        # max APIGateway repsonse size of 10MB leaving that logic
        # out for now
        response = Response(json.dumps({"Items": items}, cls=MyEncoder))
        response.content_type = 'application/json'
        response.content_length = len(response.data)
        self._request_logging(
            'dbquery',
            str(environ.get('awsgi.requester')),
            "Response Content Length -- " + str(response.content_length)
            )
        return response(environ, start_response)

    def jsonencode(self, environ, start_response):
        """
        GET /data

        Endpoint for getting collection data out of AWS
        Returns JSON that can be load by pandas
            pandas.read_json(response).set_index(['index'])
        Function call will construct DataFrame in consisent format
        """

        request = Request(environ)
        Ticker = self._tickerParameterValidation(
            request.args.get('Ticker')
            )
        day = self._dayparameterValidation(
            request.args.get('day')
            )
        if Ticker is None or day is None:
            self._request_logging(
                'decodedjson',
                str(environ.get('awsgi.requester')),
                'Bad Request Parameters')
            return Response(
                "Bad Request -- see README.md"
                )(environ, start_response)

        day = day.replace("-", "")
        items = self._dbquery(Ticker, day)
        if len(items) == 0:
            items = json.dumps({})
        else:
            items = pd.concat(list(map(
                _decodeItem, items
                ))).reset_index().to_json()
        response = Response(items)
        response.content_type = 'application/json'
        response.content_length = len(response.data)
        self._request_logging(
            'jsonencode',
            str(environ.get('awsgi.requester')),
            "Response Content Length -- " + str(response.content_length)
            )
        return response(environ, start_response)

    def initializecollection(self, environ, start_response):
        """
        POST /collect

        Enpoint function for starting collection run for companies/tickers
        that are currently in the collection list.
        """

        lambdaclient.invoke(
                FunctionName='OptionsHistory-CollectDataFunc',
                InvocationType='Event',
                Payload=json.dumps(
                    {'State': 'initialize'}
                    ).encode()
                )
        return Response(
            "Collection Run initialized {}\n".format(
                datetime.now().isoformat()
                )
            )(environ, start_response)

    def __call__(self, environ, start_response):
        # Bind the environment to the url rule map
        adapter = self.map.bind_to_environ(environ)
        try:
            endpoint, _ = adapter.match()
            return getattr(self, endpoint)(environ, start_response)
        except Exception as e:
            self._request_logging(
                str(environ.get('PATH_INFO')),
                str(environ.get('awsgi.requester')),
                str(e)
                )
            return Response(
                "Invalid Path or Parameters\n"
                )(environ, start_response)


# Get DyanamoDB Table s3 client objects
dbtable = boto3.resource('dynamodb').Table("OptionsHist")
tracked_tickers = boto3.resource('dynamodb').Table("OptionsHistTickers")
lambdaclient = boto3.client('lambda')


def handler(event, context):
    """
    Lambda Handler. wrap the event and context into WSGI application
        using awsgi. Using the werkzeug tools set to contruct a
        response.
        awsgi --> https://github.com/slank/awsgi.git
    """
    return awsgi.response(WSGIApp(), event, context)
