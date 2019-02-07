# script which downloads the molecules from a PDB file (line code HETNAM)
# and store them in Elasticsearch

import requests
import json
import pypdb

# endpoint of AWS Elasticsearch cluster
uri = 'https://vpc-budgies-3cg22sou4yelnjfivoz6qbic24.us-east-1.es.amazonaws.com/'
headers = {"Content-Type":"application/json"}

# read data from file containing Uniprot data and extract the PDB ID
with open('/home/ubuntu/spark_uniprot_result.txt', 'r') as infile:
    
    # loop through lines until end of file
    while True:
        line = infile.readline().strip('\n')
        if line == '':
            break

        else:
            dic = json.loads(line.replace("'", '"'))
            pdb_IDs = dic['pdb']
            for ID in pdb_IDs[:1]:

                # get list of ligands
                # each element is a dictionary with one key : "ligand"
                # which is a dictionary or a list of dictionaries
                ligands = pypdb.get_ligands(ID)['ligandInfo']

                # put data into Elasticsearch
                # in the 'molecules' index
                r = requests.put(uri + 'molecules/_doc/{0}?pretty'.format(ID),\
                                 headers=headers,\
                                 data = json.dumps(ligands))
