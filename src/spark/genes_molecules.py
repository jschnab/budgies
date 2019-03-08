# script which finds the list of gene IDs which have a PDB ID with
# molecules bound to the structure and saves them as a text file

import requests
import json

def get_config():
    """Read configuration file to get headers and Elasticsearch endpoint."""

    with open('config.txt', 'r') as config:
        while True:
            line = config.readline()
            if not line:
                break

            else:
                splitted = line.split('=')
                if splitted[0] == 'elasticsearch_endpoint':
                    endpoint = splitted[1].strip('\n')
                elif splitted[0] == 'headers':
                    headers = json.loads(splitted[1].strip('\n').replace("'", '"'))

    return endpoint, headers

def get_pdbids_from_text():
    """Return list of PDB IDs from text file. These PDB IDs have bound molecules."""
    pdb_ids = []
    with open('molecules_pdbset.txt', 'r') as infile:
        while True:
            line = infile.readline().strip('\n')
            if not line:
                break
            else:
                pdb_ids.append(line)

    return pdb_ids

def get_geneids(pdbid):
    """Return list of gene ids corresponding to Uniprot accessions \
which have PDB structures with bound molecules."""
    query = {"size":1, "query": {"match": {"pdb": pdbid}}, "_source": ['refseq', 'ensembl']}
    data = json.dumps(query)

    r = requests.get(endpoint + 'uniprot/_search?pretty', headers=headers, data=data)

    gene_ids = []

    if r.ok:
        hits = json.loads(r.text)['hits']['hits']
        for h in hits:
            gene_ids += h['_source']['refseq']
            gene_ids += h['_source']['ensembl']

    return gene_ids

if __name__ == '__main__':
    endpoint, headers = get_config()
    pdb_ids = get_pdbids_from_text()
    gene_set = []
    for i in pdb_ids:
        gene_set += get_geneids(i)
    gene_set = set(gene_set)
    with open('genes_molecules.txt', 'w') as outfile:
        for g in gene_set:
            outfile.write(g + '\n')
