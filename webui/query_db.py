# script which queries Elasticsearch indexes with a keyword search
# to get small molecules from protein structures

import os
import sys
import json
import requests
import subprocess
from email.message import EmailMessage
from email.utils import formatdate
from smtplib import SMTP_SSL
from ssl import create_default_context

def get_config():
    """Read configuration file to get headers and Elasticsearch endpoint."""

    home = os.getenv('HOME', 'default')

    with open(home + '/budgies/src/config.txt', 'r') as config:
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

    return home, endpoint, headers

# the following function is deprecated
def get_uniprot_geneset():
    """Get set of genes present in uniprot index of Elasticsearch."""

    gene_set = set()

    with open(home + 'budgies/src/uniprot_geneset.txt', 'r') as infile:
        while True:
            line = infile.readline()
            if line.strip('\n') == '':
                break
            else:
                gene_set.add(line.strip('\n'))

    return gene_set

def get_molecules_geneset(home):
    """Get set of genes present in uniprot index of Elasticsearch and
which correspond to PDB IDs with molecules attached to them."""

    gene_set = set()

    with open(home + '/budgies/src/genes_molecules.txt', 'r') as infile:
        while True:
            line = infile.readline().strip('\n')
            if line.strip('\n') == '':
                break
            else:
                gene_set.add(line)

    return gene_set

def build_query(raw_query):
    """Build search query to pass as data in requests.get() from
keywords passed as command line arguments."""

    operators = ['AND', 'OR', 'NOT']

    if any(op in raw_query for op in operators):
        query = '{"query": {"query_string": {"default_field": "description",\
                                           "query": "' + raw_query + '"}},\
                  "_source": ["description", "gene_ids"]}'

    else:
        query = '{"size": 10, "sort": ["_score"], \
                  "query": {"match": {"description": "' + raw_query + '"}}, \
                  "_source": ["description", "gene_ids"]}'

    return query

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

        return hits['total'], hits['hits']
    
def molecules_query(endpoint, index, headers, pdbid):
    """Search Elasticsearch "molecules" index for a PDB ID returned by the \
Uniprot search. Return number of hits and list of dictionaries \
corresponding to search hits."""

    uri = '{0}{1}/_doc/{2}'.format(endpoint, index, pdbid)

    r = requests.get(uri, headers=headers)

    if r.ok:

        return json.loads(r.text)

def save_csv(home, results):
    """Save results of Elasticsearch query as a tab-separated values file."""

    save_path = home  + '/budgies/output/molecules.csv'
    with open(save_path, 'w') as outfile:
        outfile.write('arrayexpress\tuniprot\tpdb\tmolecule\n')
        for result in results:
            outfile.write('\t'.join(result) + '\n')

def copy_to_s3(project):
    """Copy results to AWS S3 "project" folder after zipping."""

    home = os.getenv('HOME', 'default')

    # zip results
    cmd1 = 'tar -C {0}/budgies/output/ -czvf results.tar.gz \
experiment_description.txt molecules.csv'.format(home)

    # send results to S3
    cmd2 = 'aws s3 cp {0}/budgies/results.tar.gz \
s3://budgies-results/{1}/'.format(home, project)

    # delete tar file
    cmd3 = 'rm {0}/budgies/results.tar.gz'.format(home)

    # run bash commands
    subprocess.run(cmd1.split())
    subprocess.run(cmd2.split())
    subprocess.run(cmd3.split())

def send_email(project, email):
    """Send an email to the user with a link to results."""

    # message information
    msg = EmailMessage()
    text = 'Hello, the query you sent to Quick as a Batch has finished processing.\n\
You can download your results from \
https://s3.amazonaws.com/budgies-results/{0}/results.tar.gz\n\
Thank you for using Quick as a Batch!\n\nThe dev team'.format(project)
    msg.set_content(text)
    msg['Subject'] = 'Quick as a Batch finished processing your query'
    msg['From'] = 'budgies.results@gmail.com'
    msg['To'] = email
    msg['Date'] = formatdate(localtime=True)

    # SMTP session
    context = create_default_context()
    s = SMTP_SSL('smtp.gmail.com', context=context)
    password = os.getenv('GMAIL_PASSWORD', 'default')
    s.login('budgies.results@gmail.com', password)
    s.send_message(msg)
    s.quit()

def send_query(raw_query):
    """Query Elasticsearch with user input, save the results in AWS S3
and send an email to the user to allow downloading of results."""
   
    home, endpoint, headers = get_config()

    # get set of genes present in "uniprot" index of Elasticsearch
    # and which correspond to PDB IDs with molecule(s) bound to them
    # to avoid searching gene IDs which are not in the index
    gene_set = get_molecules_geneset(home)
 
    # create text file containing description of Arrayexpress experiments
    with open(home + '/budgies/output/experiment_description.txt', 'w') as outfile:
        pass

    # set of (accession, uniprot ID, PDB ID, molecule name)
    # which will be saved as csv file
    final_results = set()

    # build query with the proper structure for Elasticsearch
    query = build_query(raw_query)

    # query ArrayExpress index
    n_hits,  arrayexpress_hits = arrayexpress_query(endpoint, 'arrayexpress', headers, query)
    print('Number of hits : {0}'.format(len(arrayexpress_hits)))

    if arrayexpress_hits is not None:
        for hit in arrayexpress_hits:

            # save experiment's description
            print('Processing {0}'.format(hit['_id']))
            print('Number of genes : {0}\n'.format(len(set(hit['_source']['gene_ids']))))
            if hit['_source']['gene_ids'] != []:
                with open(home + '/budgies/output/experiment_description.txt', 'a') as outfile:
                    outfile.write(hit['_id'] + ' : ' + str(hit['_source']['description']) + '\n')

            # get genes from a hit on ArrayExpress
            genes = set(hit['_source']['gene_ids'][:100])

            for g in genes:

                # check if gene has a corresponding PDB structure with molecules bound to it
                if g in gene_set:

                    # query Uniprot index
                    n_hits_uniprot, uniprot_hits = uniprot_query(endpoint, 'uniprot', headers, g)

                    if uniprot_hits is not None:
                        for uprot_hit in uniprot_hits:

                            # get PDB IDs from a hit on Uniprot
                            pdbids = set(uprot_hit['_source']['pdb'])

                            for pdb in pdbids: 

                                # get molecules corresponding to a PDB ID
                                molecules_hits = molecules_query(endpoint, 'molecules', headers, pdb)

                                if molecules_hits is not None:
                                    for ligand in molecules_hits['_source']['ligand']:
                                        try:
                                            final_results.add((hit['_id'], uprot_hit['_id'], ligand['@structureId'], ligand['chemicalName']))
                                        except TypeError:
                                            pass
    save_csv(home, final_results)
