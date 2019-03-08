# script which extracts all PDB IDs from index "molecules" in Elasticsearch
# and saves them in a text file (one PDB ID per line)

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
                    endpoint = splitted[1].strip()
                elif splitted[0] == 'headers':
                    headers = json.loads(splitted[1].strip('\n').replace("'", '"'))

    return endpoint, headers

def get_pdbids():
    """Return list of PDB IDs corresponding to all molecules \
from Elasticsearch."""
    query = {"size":3506, "query": {"match_all": {}}}

    data = json.dumps(query)

    r = requests.get(endpoint + 'molecules/_search?pretty', headers=headers, data=data)

    pdb_ids = []

    if r.ok:
        print('Request is OK.')

        hits = json.loads(r.text)['hits']['hits']
        print('Number of hits: {0}'.format(len(hits)))
        for h in hits:
            pdb_ids.append(h['_id'])

    pdb_set = set(pdb_ids)

    return pdb_set

if __name__ == '__main__':
    endpoint, headers = get_config()
    pdb_set = get_pdbids()
    with open('molecules_pdbset.txt', 'w') as outfile:
        for g in pdb_set:
            outfile.write(g + '\n')
