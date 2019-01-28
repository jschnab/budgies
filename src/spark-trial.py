# script to try Spark in standalone mode with PDB ID extraction from Uniprot files

from pyspark import SparkConf, SparkContext
import sys
import subprocess
import re
import itertools

conf = SparkConf().setMaster("ec2-3-92-97-223.compute-1.amazonaws.com").setAppName("Spark trial")
sc = SparkContext(conf=conf)

files = sc.wholeTextFiles("s3://budgies/spark/alice/*.txt")

regex = re.compile(r" [0-9A-Z]{4}")

pdb = myfile.map(lambda pair: (pair[0].split('/')[-1]\
            .split('.txt')[0], pair[1].split('\n')))\
            .map(lambda pair: (pair[0], [i.split(';') for i in pair[1] if 'DR   PDB' in i]))\
            .map(lambda pair: (pair[0], list(itertools.chain.from_iterable(pair[1]))))\
            .map(lambda pair: (pair[0], [i for i in filter(regex.search, pair[2])]))\
            .map(lambda pair: (pair[0], [i.lstrip() for i in pair[1]]))\
            .filter(lambda pair: pair[1] != [])

pdb.collect()

sys.exit()
