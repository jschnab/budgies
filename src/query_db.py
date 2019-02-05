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
                elif splitted[0] == 'headers':
                    headers = json.loads(splitted[1].strip('\n').replace("'", '"'))

    return endpoint, headers

def get_uniprot_geneset():
    """Get set of genes present in uniprot index of Elasticsearch."""

    gene_set = set()

    with open('uniprot_geneset.txt', 'r') as infile:
        while True:
            line = infile.readline()
            if line.strip('\n') == '':
                break
            else:
                gene_set.add(line.strip('\n'))

    return gene_set

def get_molecules_geneset():
    """Get set of genes present in uniprot index of Elasticsearch and
which correspond to PDB IDs with molecules attached to them."""

    gene_set = set()

    with open('genes_molecules.txt', 'r') as infile:
        while True:
            line = infile.readline().strip('\n')
            if line.strip('\n') == '':
                break
            else:
                gene_set.add(line)

    return gene_set

def build_query():
    """Build search query to pass as data in requests.get() from
'+'-separated keywords passed as command line arguments."""

def arrayexpress_query(endpoint, index, headers, data):
    """Search Elasticsearch ArrayExpress index for experiment description \
based on endpoint, headers and query passed as data. Return number of hits \
and a list of dictionaries corresponding to search hits."""

    # build URI for query
    uri = '{0}{1}/_search'.format(endpoint, index)

    r = requests.get(uri, headers=headers, data=data)

    if r.ok:
        hits = json.loads(r.text)['hits']

        return hits['total'], hits['hits']

def uniprot_query(endpoint, index, headers, gene_id):
    """Search Elasticsearch Uniprot index for a gene ID returned by the \
ArrayExpress search. Return number of hits and list of dictionaries \
corresponding to search hits."""

    uri = '{0}{1}/_search'.format(endpoint, index)

    if 'ENSG' in gene_id:
        data = json.dumps({"size":1, "sort": ["_score"], "query": {"match": {"ensembl": gene_id}}, "_source": "pdb"})

    else:
        data = json.dumps({"size":1, "sort": ["_score"], "query": {"match": {"refseq": gene_id}}, "_source": "pdb"})

    r = requests.get(uri, headers=headers, data=data)

    if r.ok:
        hits = json.loads(r.text)['hits']
        if hits['total'] > 0:
            with open('results_query_uniprot.txt', 'a') as outfile:
                for h in hits['hits']:
                    outfile.write(json.dumps(h))

        return hits['total'], hits['hits']
    
def molecules_query(endpoint, index, headers, pdbid):
    """Search Elasticsearch "molecules" index for a PDB ID returned by the \
Uniprot search. Return number of hits and list of dictionaries \
corresponding to search hits."""

    uri = '{0}{1}/_doc/{2}'.format(endpoint, index, pdbid)

    r = requests.get(uri, headers=headers)

    if r.ok:

        return json.loads(r.text)

if __name__ == '__main__':

    # get set of genes present in "uniprot" index of Elasticsearch
    # and which correspond to PDB IDs with molecule(s) bound to them
    # to avoid searching gene IDs which are not in the index
    gene_set = get_molecules_geneset()
    
    endpoint, headers = get_config()

    #query = build_query()
    query = '{"size": 1, "sort": ["_score"], \
              "query": {"match": {"description": "diabetes"}}, \
              "_source": ["description", "gene_ids"]}'

    # set of (accession, uniprot ID, PDB ID, molecule name)
    final_results = set()

    # query ArrayExpress index
    n_hits,  arrayexpress_hits = arrayexpress_query(endpoint, 'arrayexpress', headers, query)

    if arrayexpress_hits is not None:
        for hit in arrayexpress_hits[:1]:

            # get genes from a hit on ArrayExpress
            genes = hit['_source']['gene_ids']

            for g in genes[:100]:

                # check if gene has a corresponding PDB structure with molecules bound to it
                if g in gene_set:

                    # query Uniprot index
                    n_hits_uniprot, uniprot_hits = uniprot_query(endpoint, 'uniprot', headers, g)

                    if uniprot_hits is not None:
                        for uprot_hit in uniprot_hits:

                            # get PDB IDs from a hit on Uniprot
                            pdbids = uprot_hit['_source']['pdb']

                            for pdb in pdbids: 

                                # get molecules corresponding to a PDB ID
                                molecules_hits = molecules_query(endpoint, 'molecules', headers, pdb)

                                if molecules_hits is not None:
                                    for ligand in molecules_hits['_source']['ligand']:
                                        #print('PDB : {0} --- Name : {1}'.format(ligand['@structureId'], ligand['chemicalName']))
                                        try:
                                            final_results.add((hit['_id'], uprot_hit['_id'], ligand['@structureId'], ligand['chemicalName']))
                                        except TypeError:
                                            pass

    for result in final_results:
        print(result)
