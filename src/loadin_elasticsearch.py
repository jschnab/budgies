# script which parses text files from ArrayExpress and Uniprot
# and load data in Elasticsearch

# get information files of all ArrayExpress experiments
# as pair (file_name, file_content)
info_files = sc.wholeTextFiles('s3n://budgies/arrayexpress2/*/*.idf.txt')

#get pairs (accession, experiment_description)
descriptions = info_files.map(lambda pair: (pair[0].split('/')[-1], pair[1]))\
                         .map(lambda pair: (pair[0], pair[1].split('\n')))\
                         .map(lambda pair: (pair[0], [i for i in pair[1] if 'Description' in i]))\
                         .map(lambda pair: (pair[0], ' '.join(pair[1].split('\t')[1:])))

# get list of accessions in budgies/arrayexpress2 from S3
bash_result = subprocess.run(['aws', 's3', 'ls', 'budgies/arrayexpress2/'], stdout=subprocess.PIPE)
decoded = bash_result.stdout.decode()
mylist = decoded.split('\n')
folders = []
for i in mylist:
    search = re.search('E-[A-Z]{4}-[0-9]*', i)
    if search:
        folders.append(search.group(0))

# file names
bash_result = subprocess.run(['aws', 's3', 'ls', 'budgies/arrayexpress2/<folder>/'], stdout=subprocess.PIPE)
decoded = bash_result.stdout.decode()
lines = decoded.split('\n')
files = [l.split(' ')[-1] for l in lines][:-1]
pattern1 = '.*(?<!README)\.txt'
pattern2 = '.*(?<!idf)\.txt'
pattern3 = '.*(?<!sdrf)\.txt'
files_to_process = []
for f in files:
    search = re.search(pattern1, f) and re.search(pattern2, f) and re.search(pattern3, f)
    if search:
        files_to_process.append(search.group(0))

# genes ids (Ensembl or RefSeq format)
path = 's3n://budgies/arrayexpress2/E-GEOD-26880/'
for f in files_to_process:
    myfile = sc.textFile(path + f)
    ids = myfile.flatMap(lambda line: line.split('\t')).filter(lambda column: re.search('ENSG[0-9]{11}|[NX]M_[0-9]+', column))
    ids_list = ids.collect()

# get pdb ids from Uniprot files
regex = re.compile(r' [0-9A-Z]{4}')
pdb = myfile.map(lambda pair: (pair[0].split('/')[-1]\
            .split('.txt')[0], pair[1].split('\n')))\
            .map(lambda pair: (pair[0], [i.split(';') for i in pair[1] if 'DR   PDB' in i]))\
            .map(lambda pair: (pair[0], list(itertools.chain.from_iterable(pair[1]))))\
            .map(lambda pair: (pair[0], [i for i in filter(regex.search, pair[2])]))\
            .map(lambda pair: (pair[0], [i.lstrip() for i in pair[1]]))\
            .filter(lambda pair: pair[1] != [])

# another way to get file names
aws s3 ls s3://budgies/spark/E-GEOD-12946/ | awk -F' +[0-9]+ ' '{print $2}' | sed '/^[[:space:]]*$/d' | sed -r '/^\s*$/d'


#===============#
### OLD IDEAS ###
#===============#
# get description of the experiment
description = files.filter(lambda pair: pair[0].endswith('idf.txt'))\
                   .flatMap(lambda pair: pair[1].split('\n'))

                   .flatMap(lambda text: text.split('\n'))\
                   .filter(lambda line: re.match('^Experiment Description', line))\
                   .flatMap(lambda line: line.split('\t'))\
                   .collect()

# get experimental files (not .README nor .idf.txt nor .sdrf.txt)
exp_files = sc.wholeTextFiles('s3n://budgies/arrayexpress2/*/\.*(?<!README|idf|sdrf).txt')
