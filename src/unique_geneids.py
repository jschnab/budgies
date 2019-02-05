# script which extracts all gene ids from index "Uniprot" in Elasticsearch
# and saves them in a text file (one gene id per line)

import requests
import json

def get_config():
    """Read configuration file to get headers and Elasticsearch endpoint."""

    with open('config.txt', 'r') as config:
        while True:
            line = config.readline()
            if line =='':
                break

            else:
                splitted = line.split('=')
                if splitted[0] == 'elasticsearch_endpoint':
                    endpoint = splitted[1].strip('\n')
                elif splitted[0] == 'headers':
                    headers = json.loads(splitted[1].strip('\n').replace("'", '"'))

    return endpoint, headers

def get_geneids():
    """Return list of gene ids corresponding to all Uniprot accession \
from Elasticsearch."""

    data = json.dumps({"size":6535, "query": {"match_all": {}}, "_source": ['refseq', 'ensembl']})

    r = requests.get(endpoint + 'uniprot/_search?pretty', headers=headers, data=data)

    gene_ids = []

    if r.ok:
        print('Request is OK.')

        hits = json.loads(r.text)['hits']['hits']
        print('Number of hits: {0}'.format(len(hits)))
        for h in hits:
            gene_ids += h['_source']['refseq'] + h['_source']['ensembl']  

    gene_set = set(gene_ids)

    return gene_set

if __name__ == '__main__':
    endpoint, headers = get_config()
    gene_set = get_geneids()
    with open('uniprot_geneset.txt', 'w') as outfile:
        for g in gene_set:
            outfile.write(g + '\n')

