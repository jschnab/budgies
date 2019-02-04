# script which queries Elasticsearch indexes with a keyword search
# to get small molecules from protein structures

import os
import sys
import getopt
import json
import requests

def get_config():
    """Read configuration file to get headers and Elasticsearch endpoint."""
    with open('config.txt', 'r') as config:
        while True:
            line = config.readline()
            if line == '':
                break

            else:
                splitted = line.split('=')
                if splitted[0] == 'elasticsearch_endpoint':
                    endpoint = splitted[1].strip('\n')
                elif splitted[1] == 'headers':
                    headers = json.loads(splitted[1].strip('\n').replace("'", '"'))

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

if __name__ == '__main__':
    
    endpoint, headers = get_config()

    #query = build_query()
    query = '{"query": {"match": {"description": "diabetes"}}, "_source": ["description", "gene_ids"}'


    arrayexpress_hits = arrayexpress_query(endpoint, 'arrayexpress', headers, query)
    if arrayexpress_hits is not None:

        if type(arrayexpress_hits) == 'dict':
            n_hits = len(arrayexpress_hits)
            print('Got {0} hits from ArrayExpress.'.format(n_hits))
            final_results = [{}] * n_hits
            for i, hit in enumerate(array_express hits):
                final_results[i]['ae_acc'] = hit['_id']
                final_results[i]['description'] = hit['_source']['description']
                final_results[i]['gene_ids'] = hit['_source']['gene_ids']

                print(final_results[i]['ae_acc'])
                print(final_results[i]['description'])
                print(len(final_results[i]['gene_ids']))

        elif type(arrayexpress_hits) == 'list':
            print('Got 1 hit from ArrayExpress.')
            final_results = [{}]
            final_results[i]['ae_acc'] = hit['_id']
            final_results[i]['description'] = hit['_source']['description']
            final_results[i]['gene_ids'] = hit['_source']['gene_ids']


    #uniprot_results = uniprot_query(endpoint, 'uniprot', headers, data)
