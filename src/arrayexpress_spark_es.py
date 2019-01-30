# script which extracts ArrayExpress description and gene ids from text files in an
# ArrayExpress accession's folder stored in S3 and saves results as a text file
# to be processed further for storage into Elasticsearch

import getopt
from pyspark import SparkConf, SparkContext
import sys
import subprocess
import re
import itertools
import json

# get list of folders
def print_help():
    """Print help."""

    help_text = \
"""Get experiment descriptions and gene identification numbers from\
 ArrayExpress accessions and load data into Elasticsearch.

    python3 arrayexpress_spark_es.py -f[accessions list file] -i[Elasticsearch index]

    Please provide an option for the accessions list file and Elasticsearch index.

    -f, --file
    Full path to the text file containing accession names.

    -i, --index
    Name of the index (i.e. database) to store data in Elasticsearch."""

    print(help_text)

def get_args():
    """Get arguments passed when the script is run at the command line."""

    try:
        opts, args = getopt.getopt(sys.argv[1:],
                'f:i:h',
                ['file=', 'index=', 'help'])

    except getopt.GetoptError as e:
        print(e)
        sys.exit(2)

    accessions_file = None

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_help()
            sys.exit()

        elif opt in ('-f', '--file'):
            accessions_file = arg

    if accessions_file == None:
        print('Please provide a file containing the accessions list.')
        sys.exit()

    return accessions_file

def get_files(folder):
    """Get list of experiments files (exclude README, idf and sdrf) from an S3 folder."""

    # get S3 folder content from BASH command line
    bash_cmd = 'aws s3 ls budgies/{0}/'.format(folder)
    bash_result = subprocess.run(bash_cmd.split(), stdout=subprocess.PIPE)
    decoded = bash_result.stdout.decode()
    lines = decoded.split('\n')
    files = [l.split()[-1] for l in lines][-1]

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
                            .filter(lambda column: re.match('ENSG[0-9]{11}|[NX]M_[0-9]', column)),

    # collect gene ids
    gene_ids = gene_ids_RDD.collect()

    return gene_ids

def get_description(folder):
    """Return description of a accession."""

    file_path = 's3n://budgies/arrayexpress2/{0}/*.idf.txt'
    text_file = sc.textFile(file_path)

    #transformation on RDD
    description_RDD = text_file.filter(lambda line: 'Experiment Description' in line),\
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
        outfile.write(str(dic) + '\n')

def store_es(index, headers, dic):
    """Store results dictionary in Elasticsearch."""
    data = json.dumps(dic)
    uri = 'http://localhost:9200/{0}/_doc/{1}'.format(index, dic['accession'])
    r = requests.put(uri, headers=headers, data=data)

if __name__ == '__main__':

    # headers for later storage in Elasticsearch
    headers = {'Content-Type': 'application/json'}
    
    # get file with accessions names and index of Elasticsearch
    accessions_file, index = get_args()

    # convert accessions_file content into list
    accessions = []
    with open(accessions_file, 'r') as acc_file:

        # loop until end of file
        while True:
            acc = acc_file.readline

            if acc = '':
                break

            else:
                # extract data from accession files
                description = get_description(acc)
                files = get_files(acc)
                gene_ids = get_gene_ids(files[0])
                for f in files[1:]:
                    gene_ids += get_gene_ids(f)

                # make dictionary of results for one accession
                result = return_dic(acc, description, gene_ids)

                # save dictionary in text file
                store_txt(result)

                # store result in Elasticsearch
                store_es(index, headers, result)
