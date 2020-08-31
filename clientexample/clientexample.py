import requests
import pandas as pd
from datetime import datetime
import json
import numpy as np
import base64
import sys
import re

if len(sys.argv) > 1:
    if re.match('[A-Z.]+', sys.argv[1]):
        EXAMPLETICKER = sys.argv[1]
    else:
        EXAMPLETICKER = 'WMT'
else:
    EXAMPLETICKER = 'WMT'

# Get the url for the APIEndpoint created when stack was deployed
with open("APIEndpoint", 'r') as fp:
    endpoint = fp.read()

# Using the Data Enpoint
response = requests.get(endpoint[:-1] + "/data", params={
    "Ticker": EXAMPLETICKER,
    "day": datetime.utcnow().isoformat()[:10]})

print("Total Response Bytes /data -- " + str(len(response.text.encode())))

data = pd.read_json(response.text)

if data.empty is True:
    print("""
No Data has been collected on example Ticker {}, open the file and change it
to a ticker that dataa has been collected for. Or see README.md to learn how
to add companies to the list the stack will collect data on.
""".format(EXAMPLETICKER))

    exit()
else:
    data = pd.read_json(response.text).set_index(['index'])
    print(data.head(10))

print("\n\n")
response = requests.get(
    endpoint[:-1] + "/data/encoded",
    params={"Ticker": EXAMPLETICKER,
            "day": datetime.utcnow().isoformat()[:10]})

########################################################################
# Function that will decode the encoded response to the same data frame


def decodeResponse(responseString):

    def _decodeOptionsTable(record):
        """
        Helper Function. Transform the Base64 encoded table into Json object
        that can be loaded through pandas.read_json()
        Inputs:
        ----------------------------------------------------------------
        record --> json string that is stored in DynamoDB as attributes
            'calls' or 'puts'

        Outputs:
        ----------------------------------------------------------------
        dict, int --> python dictionary with keys for the columns of the
            table value sequence of values
        """

        def _last_trade_date_transformation(string):
            """ Helper Function put last trade date in proper format """
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
        # From here you can just pd.DataFrame(frame)
        # Note here for future use when I use this for something else.

        # Select out columns that show in self.labelMap
        # selected_frame = {}
        # for key, val in self.labelMap.items():
        #     selected_frame[val] = frame[key]

        # # transform the lasttradedate values for the database
        # selected_frame['lasttradedate'] = list(map(
        #     self._last_trade_date_transformation,
        #     selected_frame['lasttradedate']
        #     ))
        # return selected_frame, len(table)

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

    items = json.loads(responseString).get("Items")
    items = list(map(_decodeItem, items))
    return pd.concat(items)


print("Total Respose Bytes /data/encoded endpoint -- "
      + str(len(response.text.encode())))
data = decodeResponse(response.text)
print(data.head(10))
