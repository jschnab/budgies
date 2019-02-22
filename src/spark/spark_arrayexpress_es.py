# script which extracts ArrayExpress description and gene ids from text files in an
# ArrayExpress accession's folder stored in S3 and saves results as a text file
# to be processed further for storage into Elasticsearch

import os
from pyspark import SparkConf, SparkContext
import sys
import subprocess
import re
import json
from elasticsearch import Elasticsearch

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

    # get Elasticsearch cluster IP addresses
    hosts = []
    hosts.append(os.getenv('MASTER_IP', 'default'))
    hosts.append(os.getenv('WORKER1_IP', 'default'))
    hosts.append(os.getenv('WORKER2_IP', 'default'))
    hosts.append(os.getenv('WORKER3_IP', 'default'))

    # get Elasticsearch credentials
    credentials = []
    credentials.append(os.getenv('ES_ACCESS_KEY', 'default'))
    credentials.append(os.getenv('ES_SECRET_ACCESS_KEY', 'default'))

    return accessions, index, hosts, credentials

def get_files(folder):
    """Get list of experiments files (exclude README, idf and sdrf) from an S3 folder."""

    # get S3 folder content from BASH command line
    bash_cmd = 'aws s3 ls budgies/arrayexpress2/{0}/'.format(folder)
    bash_result = subprocess.run(bash_cmd.split(), stdout=subprocess.PIPE)
    decoded = bash_result.stdout.decode()
    lines = decoded.split('\n')
    files = [l.split()[-1] for l in lines[:-1]]

    # regex to exclude files mentioned above based on negative lookbehind
    r1 = '.*(?<!README)\.txt$'
    r2 = '.*(?<!idf)\.txt$'
    r3 = '.*(?<!sdrf)\.txt$'

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
                            .filter(lambda column: re.match('ENSG[0-9]{11}|[NX]M_[0-9]+', column))\
                            .map(lambda column: re.match('ENSG[0-9]{11}|[NX}M_[0-9]+', column).group(0))

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

def store_es(es, index, dic):
    """Store results dictionary in Elasticsearch."""

    result = es.index(index=index, doc_type='_doc', body=dic, id=dic['accession'])

    with open('es_log.txt', 'a') as outfile:
        if result is not None:
            outfile.write(json.loads(result) + '\n')

if __name__ == '__main__':

    # setting Spark configuration and context
    conf = SparkConf().setMaster("spark://ec2-3-92-97-223.compute-1.amazonaws.com:7077").setAppName("ArrayExpress to Elasticsearch")
    sc = SparkContext(conf=conf)

    # get file with accessions names and index of Elasticsearch
    accessions_file, index, hosts, credentials = get_config()

    es = Elasticsearch(hosts, http_auth=credentials, port=9200, sniff_on_start=True)

    # convert accessions_file content into list
    accessions = []
    with open(accessions_file, 'r') as acc_file:

        # loop until end of file
        while True:
            acc = acc_file.readline().strip('\n')

            if acc == '':
                break

            else:
                accessions.append(acc)

    # loop over accession names and process them
    for acc in accessions:

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
            store_es(es, index, result)

    sys.exit()
