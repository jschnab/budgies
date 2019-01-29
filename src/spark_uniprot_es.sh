# Spark job which parses Uniprot files and place data in Elasticsearch
# (accession, {pdb, ensembl, refseq)
# index: uniprot

$SPARK_HOME/bin/pyspark --jars elasticsearch-hadoop-6.5.4/dist/elasticsearch-hadoop-6.5.4.jar
                        --master spark://ec2-3-92-97-223.compute-1.amazonaws.com:7077
                        spark_uniprot_es.py
