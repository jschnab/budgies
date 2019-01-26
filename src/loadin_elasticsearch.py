# script which parses text files from ArrayExpress and Uniprot
# and load data in Elasticsearch

# get files of an ArrayExpress experiment
files = sc.wholeTextFiles('s3n://budgies/arrayexpress2/<accession>')

# get description of the experiment
description = files.filter(lambda pair: pair[0].endswith('idf.txt'))\
                   .map(lambda pair: ''.join(pair[1]))\
                   .flatMap(lambda text: text.split('\n'))\
                   .filter(lambda line: re.match('^Experiment Description', line))\
                   .flatMap(lambda line: line.split('\t'))\
                   .collect()
