# script which downloads the molecules from a PDB file (line code HETNAM)
# and store them in Elasticsearch

import json
import pypdb

# endpoint of AWS Elasticsearch cluster
uri = 'https://vpc-budgies-3cg22sou4yelnjfivoz6qbic24.us-east-1.es.amazonaws.com/'
headers = {"Content-Type":"applicationjson"}

# read data from file containing Uniprot data and extract the PDB ID
with open('~/spark_uniprot_result.txt', 'r') as infile:
    
    # loop through lines until end of file
    while True:
        line = infile.readline().strip('\n')
        if line == '':
            break

        else:
            dic = json.loads(line.replace("'", '"'))
            pdb_IDs = dic['pdb']
            for ID in pdb_IDs:

                # get list of ligands
                # each element is a dictionary
                ligands = pypdb.get_ligands(ID)['ligandInfo']['ligand']

                # put data into Elasticsearch
                # in the 'molecules' index
                r = requests.put(uri + 'molecules/_doc/{0}?pretty'.format(ID),
                                 headers=headers,
                                 data = json.dumps(ligands))
