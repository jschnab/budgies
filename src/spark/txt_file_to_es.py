# script which reads a text file containing a dictionary on each line
# and put it into Elasticsearch

import json
import requests

url = 'http://vpc-budgies-3cg22sou4yelnjfivoz6qbic24.us-east-1.es.amazonaws.com/uniprot/_doc/'
headers = {'Content-Type': 'application/json'}

with open('spark_uniprot_result.txt', 'r') as infile:
    while True:
        line = infile.readline()
        if line == '':
            break
        else:
            dic = json.loads(line.strip('\n').replace("'", '"'))
            identifier = dic['accession']
            r = requests.put(url + identifier, headers=headers, data=json.dumps(dic))
            if not r.ok:
                with open('errors_txt_to_es.txt', 'a') as log:
                    log.write(r.text + '\n')
