#! /bin/bash

TICKERS="AAPL AXP BA CAT CSCO CVX DIS DOW GS HD IBM INTC JNJ JPM KO MCD MMM MRK MSFT NKE PFE PG RTX TRV UNH V VZ WBA WMT XOM"

for t in $(echo $TICKERS); do
	curl -X POST $(cat APIEndpoint)"/tickers?Ticker=${t}"
done

# Curl example of DELETING a ticker from the Collection
# curl -X "DELETE" $(cat APIEndpoint)'/tickers?Ticker=AAPL'
