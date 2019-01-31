# script to try Spark in standalone mode with PDB ID extraction from Uniprot files

from pyspark import SparkConf, SparkContext
import sys
import re
import itertools
import json

# setup Spark context
conf = SparkConf().setMaster("spark://ec2-3-92-97-223.compute-1.amazonaws.com:7077").setAppName("Uniprot-to-txt")
sc = SparkContext(conf=conf)

# read files
files = sc.wholeTextFiles("s3n://budgies/uniprot/*.txt")

# define regular expression to parse text file
re_pdb = re.compile(r'[0-9A-Z]{4}')
re_ens = re.compile(r'ENSG[0-9]{11}')
re_ref = re.compile(r'[NX]M_[0-9]+')

# make a dictionary out of Spark output for insertion into Elasticsearch
def return_dic(results):
    """Return a dictionary with keys accession, description and gene ids\
for saving as line in a text file. This makes storing into Elasticsearch\
easier when reading the text file."""
    dic = {}
    dic["accession"] = results[0]
    dic["pdb"] = results[1]
    dic["ensembl"] = results[2]
    dic["refseq"] = results[3]
    
    return dic

# parse the Uniprot text file
uniprot_RDD = files.map(lambda pair: (pair[0].split('/')[-1].split('.txt')[0],\
                                      pair[1].split('\n')))\
                   .map(lambda pair: (pair[0],\
                                      [i.split(';') for i in pair[1] if 'DR   PDB;' in i],\
                                      [i.split(';') for i in pair[1] if 'DR   Ensembl' in i],\
                                      [i.split(';') for i in pair[1] if 'DR   RefSeq' in i]))\
                   .map(lambda pair: (pair[0],\
                                      list(itertools.chain.from_iterable(pair[1])),\
                                      list(itertools.chain.from_iterable(pair[2])),\
                                      list(itertools.chain.from_iterable(pair[3]))))\
                   .map(lambda pair: (pair[0],\
                                      [i for i in filter(re_pdb.search, pair[1])],\
                                      [i for i in filter(re_ens.search, pair[2])],\
                                      [i for i in filter(re_ref.search, pair[3])]))\
                   .map(lambda pair: (pair[0],\
                                      [i.lstrip() for i in pair[1]],\
                                      [i.lstrip().split('.')[0] for i in pair[2]],\
                                      [i.lstrip().split('.')[0] for i in pair[3]]))\
                   .filter(lambda pair: pair[1] != [] and pair[2] != [] \
                                        or pair[1] != [] and pair[3] != [])

# Spark action
result = uniprot_RDD.collect()

# write results to a text file
with open('/home/ubuntu/spark_uniprot_result.txt', 'w') as outfile:
    for i in result:
        outfile.write(str(return_dic(i)) + '\n')

# terminate the Spark job
sys.exit()
