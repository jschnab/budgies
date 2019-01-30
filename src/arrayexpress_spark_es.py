# script which loads into Elasticsearch description and gene ids from text files in an
# ArrayExpress accession's folder stored in S3

import getopt
from pyspark import SparkConf, SparkContext
import sys
import subprocess
import re
import itertools
import json
import hashlib

# get list of folders
def print_help():
    """Print help."""

    help_text = \
"""Get experiment descriptions and gene identification numbers from\
 ArrayExpress accessions and load data into Elasticsearch.

    python3 arrayexpress_spark_es.py -f[accessions list file]

    Please provide an option for the accessions list file.

    -a, --accessions
    Full path to the text file containing accession names."""

    print(help_text)

def get_args():
    """Get arguments passed when the script is run at the command line."""

    try:
        opts, args = getopt.getopt(sys.argv[1:],
                'a:f:h',
                ['accessions=', 'file=', 'help'])

    except getopt.GetoptError as e:
        print(e)
        sys.exit(2)

    accessions = None

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_help()
            sys.exit()

        elif opt in ('-a', '--accessions'):
            accessions = arg

    if accessions == None:
        print('Please provide a file containing the accessions list.')
        sys.exit()

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
