# script to try Spark in standalone mode with PDB ID extraction from Uniprot files

from pyspark import SparkConf, SparkContext
import sys
import subprocess
import re
import itertools
import json
import hashlib

# setup Spark context
conf = SparkConf().setMaster("spark://ec2-3-92-97-223.compute-1.amazonaws.com:7077").setAppName("Spark trial")
sc = SparkContext(conf=conf)

# read files
files = sc.wholeTextFiles("s3n://budgies/spark/alice/*.txt")

# define regular expression to parse text file
re_pdb = re.compile(r'[0-9A-Z]{4}')
re_ens = re.compile(r'ENSG[0-9]{11}')
re_ref = re.compile(r'[NX]M_[0-9]+')

# convert parsed text file to a tuple (accession, dictionary)
# for input in Elasticsearch
def serialize(RDD):
    dic = {}
    dic['accession'] = RDD[0]
    dic['pdb'] = RDD[1]
    dic['ensembl'] = RDD[2]
    dic['refseq'] = RDD[3]
    hash_obj = json.dumps(RDD).encode('ascii', 'ignore')
    dic['doc_id'] = hashlib.sha224(hash_obj).hexdigest()
    return (dic['doc_id'], json.dumps(dic))

# parse the Uniprot text file
uniprot_RDD = files.map(lambda pair: (pair[0].split('/')[-1].split('.txt')[0],\
                                      pair[1].split('\n')))\
                   .map(lambda pair: (pair[0],\
                                      [i.split(';') for i in pair[1] if 'DR   PDB' in i],\
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
                   .filter(lambda pair: pair[1] != [] and pair[2] != [] and pair[3] != [])

# transform parsed text file for input into Elastic search
uniprot_dict = uniprot_RDD.map(serialize)
#print('\n\n' + str(uniprot_dict.collect()) + '\n\n')

# Spark action
#result = uniprot_RDD.collect()

# write results to a text file
with open('/home/ubuntu/spark_uniprot_result.txt', 'w') as outfile:
    outfile.write(result)

# setup Elasticsearch write configuration 
es_write_conf = {
        'es.nodes': 'localhost',
        'es.port': '9200',
        'es.resource': 'test/apache',
        'es.input.json': 'yes',
        'es.mapping.id': 'doc_id'
        }

# save in Elasticsearch
#uniprot_dict.saveAsNewAPIHadoopFile(path='-',
#                              outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat',
#                              valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable',
#                              conf=es_write_conf)

# terminate the Spark job
sys.exit()
