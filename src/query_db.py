# script which queries Elasticsearch indexes with a keyword search
# to get small molecules from protein structures

import os
import sys
import getopt
import json
import requests

def get_config():
    """Read configuration file to get headers and Elasticsearch endpoint."""

def build_query():
    """Build search query to pass as data in requests.get() from
'+'-separated keywords passed as command line arguments."""

def arrayexpress_query(endpoint, index, headers, data):
    """Search Elasticsearch ArrayExpress index for experiment description \
based on endpoint, headers and query passed as data. Return a list of dictionaries \
corresponding to search hits."""

    # build URI for query
    uri = '{0}{1}/_search'.format(endpoint, index)

    r = requests.get(uri, headers, data)
    if r.ok:
        return json.loads(r.text)['hist']['hits']

def uniprot_query(endpoint, index, headers, data):
    """Search Elasticsearch Uniprot index for gene IDs returned by the \
ArrayExpress search. Return PDB IDs."""
