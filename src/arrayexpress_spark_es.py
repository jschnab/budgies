# script which extracts ArrayExpress description and gene ids from text files in an
# ArrayExpress accession's folder stored in S3 and saves results as a text file
# to be processed further for storage into Elasticsearch

import os
import sys
import getopt
from pyspark import SparkConf, SparkContext
import sys
import subprocess
import re
import itertools
import json
import requests
import time

# get list of folders
def print_help():
    """Print help."""

    help_text = \
"""Get experiment descriptions and gene identification numbers from\
 ArrayExpress accessions and load data into Elasticsearch.

    Please provide a configuration file containing the full path to the file\
containing accession names and the name of the index. See below for the formatting.

    accession_file=<accession file path>
    index=<index name>
"""

    print(help_text)

def get_config():
    """Return path for the file containing accessions and Elasticsearch index."""
    with open('config.txt', 'r') as config:
        while True:
            line = config.readline()
            if line == '':
                break
            else:
                splitted = line.split('=')
                # get the sorted accessions file if it exists
                if splitted[0] == 'accession_file':
                    accessions = splitted[1].strip('\n')
                elif splitted[0] == 'index':
                    index = splitted[1].strip('\n')

    if os.path.exists(sys.path[0] + '/sorted_folders.txt'):
        accessions = sys.path[0] + '/sorted_folders.txt'

    return accessions, index

def get_files(folder):
    """Get list of experiments files (exclude README, idf and sdrf) from an S3 folder."""

    # get S3 folder content from BASH command line
    bash_cmd = 'aws s3 ls budgies/arrayexpress2/{0}/'.format(folder)
    bash_result = subprocess.run(bash_cmd.split(), stdout=subprocess.PIPE)
    decoded = bash_result.stdout.decode()
    lines = decoded.split('\n')
    files = [l.split()[-1] for l in lines[:-1]]

    # regex to exclude files mentioned above based on negative lookbehind
    r1 = '.*(?<!README)\.txt'
    r2 = '.*(?<!idf)\.txt'
    r3 = '.*(?<!sdrf)\.txt'

    files_to_process = []
    for f in files:
        search = re.search(r1, f) and re.search(r2, f) and re.search(r3, f)
        if search:
            files_to_process.append(search.group(0))

    return files_to_process

def get_gene_ids(folder, exp_file):
    """Return list of gene ids from a file."""
    
    file_path = 's3n://budgies/arrayexpress2/{0}/{1}'.format(folder, exp_file)
    text_file = sc.textFile(file_path)

    # transformations on RDD
    gene_ids_RDD = text_file.flatMap(lambda line: line.split('\t'))\
                            .filter(lambda column: re.match('ENSG[0-9]{11}|[NX]M_[0-9]', column))

    # collect gene ids
    gene_ids = gene_ids_RDD.collect()

    return gene_ids

def get_description(folder):
    """Return description of an accession."""

    file_path = 's3n://budgies/arrayexpress2/{0}/*.idf.txt'.format(folder)
    text_file = sc.textFile(file_path)

    #transformation on RDD
    description_RDD = text_file.filter(lambda line: 'Experiment Description' in line)\
                               .map(lambda line: line.split('\t')[1])

    description = description_RDD.collect()

    return description

def return_dic(accession, description, gene_ids):
    """Return a dictionary with keys accession, description and gene ids\
for saving as line in a text file. This makes storing into Elasticsearch\
easier when reading the text file."""
    dic = {}
    dic["accession"] = accession
    dic["description"] = description
    dic["gene_ids"] = gene_ids

    return dic

def store_txt(dic):
    """Store results dictionary as a line in a text file, as a safety\
backup if Spark job fails."""
    with open('/home/ubuntu/spark_arrayexpress_result.txt', 'a') as outfile:
        outfile.write(str(dic['accession']) + '\n')

def store_es(index, headers, dic):
    """Store results dictionary in Elasticsearch."""
    data = json.dumps(dic)

    uri = 'https://vpc-budgies-3cg22sou4yelnjfivoz6qbic24.us-east-1.es.amazonaws.com\
/{0}/_doc/{1}'.format(index, dic['accession'])

    r = requests.put(uri, headers=headers, data=data)
    with open('es_log.txt', 'a') as outfile:
        outfile.write(str(r.status_code))
        if r.text is not None:
            outfile.write(r.text + '\n')
        else:
            outfile.write('\n')

if __name__ == '__main__':

    # setting Spark configuration and context
    conf = SparkConf().setMaster("spark://ec2-3-92-97-223.compute-1.amazonaws.com:7077").setAppName("ArrayExpress to Elasticsearch")
    sc = SparkContext(conf=conf)

    # headers for later storage in Elasticsearch
    headers = {'Content-Type': 'application/json'}
    
    # get file with accessions names and index of Elasticsearch
    accessions_file, index = get_config()

    # convert accessions_file content into list
    accessions = []
    with open(accessions_file, 'r') as acc_file:

        # loop until end of file
        while True:
            acc = acc_file.readline().strip('\n')

            if acc == '':
                break

            else:
                # extract data from accession files
                description = get_description(acc)
                files = get_files(acc)

                # process files if there are any
                if files != []:
                    gene_ids = get_gene_ids(acc, files[0])

                    if len(files) >= 2:
                        for f in files[1:]:
                            gene_ids += get_gene_ids(acc, f)

                    # make dictionary of results for one accession
                    result = return_dic(acc, description, gene_ids)

                    # save dictionary in text file
                    store_txt(result)

                    # store result in Elasticsearch
                    store_es(index, headers, result)
