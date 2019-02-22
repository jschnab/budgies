# Quick as a Batch (aka budgies) 

### Facilitating drug discovery through databases integration

## Table of Contents
1. [Introduction](README.md#introduction)
2. [Motivation](README.md#motivation)
3. [Approach](README.md#approach)
4. [Pipeline](README.md#pipleline)
5. [Challenges](README.md#challenges)
6. [Future improvements](README.md#improvements)
7. [Repository structure](README.md#repository)
8. [Support](README.md#support)

## Introduction

Quick as a Batch is a data analysis tool which aims at facilitating drug discovery by integrating biological databases with Apache Spark. It provides a web interface built with Flask to query an Elasticsearch database and provides faster results compared to querying individual databases. This project was achieved in three weeks while I was a fellow at Insight Data Science, a company helping people with skills in software engineering to transition to data engineering.

## Motivation

There is a need for drug discovery. Drug performance is impaired by antibiotic resistance in micro-organisms, drug resistance in cancer cells, etc. Also, less than 1% of drug candidates will actually prove safe and able to cure, so pharmaceutical companies need to screen thousands of drugs. Moreover, technical advances in biology have vastly increased the breadth of available data, but there is poor integration between datasets. We can increase the value of these data by uncovering how they related.

## Approach

To solve the aforementioned issues, Quick as a Batch merges gene expression data from the [ArrayExpress database](www.ebi.ac.uk/arrayexpress/) with protein structure data from the [Protein Data Bank](www.rcsb.org) (PDB). The link between the two is provided by the [Uniprot database](www.uniprot.org), which can be viewed as an index containing references to genes and proteins. From ArrayExpress, I extract description of experiments and a list of genes resulting from the experiment. Typically, a user would query the experiment descriptions for keywords (e.g. a disease name) and obtain a gene list. This gene list is converted into a protein list thanks to Uniprot. Finally, the protein list is used to query PDB which results in a list of protein structures containing molecules. A data scientist can then use the list of molecules to design new drugs by searching structurally similar molecules.

## Pipeline

<img src='pipeline.jpg' width='800' alt='pipeline'>

The data is ingested from the external databases thanks to scripts automating API queries. Data from ArrayExpress (1 TB) and Uniprot (1 GB) is stored in Amazon Web Services Simple Storage System (AWS S3). These data represent a fraction of the original databases corresponding only to data relevant to human. Data from the PDB is downloaded directly to Elasticsearch with the help of the [PyPDB library](github.com/williamgilpin/pypdb), since data volume is more limited. All the data of interest are in text files.

Spark is used to parse the text files and extract useful information from them (list of genes, of proteins, etc.). The output is dictionaries which are converted to JSON for storage in Elasticsearch.

Elasticsearch contains three indices: arrayexpress (genes), uniprot (proteins) and molecules. When a user sends a query to Quick as a Batch, keywords are used to query arrayexpress on experiment descriptions. Matches give a gene list which is used to query the uniprot index. Finally, the results returned by the uniprot index are used to query the molecules index, yielding a list of molecules. There is 100% overlap between uniprot and molecules indices.

I used Flask to build a web interface on which users can enter keywords (e.g. a disease such as diabetes), a project name and an email address to be alerted when their computation is completed.

## Challenges

All the data is made of complex and heterogeneous text files. These files are not optimised for computer processing since they were designed to be understandable by humans, as many users of these files read them by eye, without any computer-aided parsing. As a result, parsing these files was challenging, and achieved with regular expressions. Protein structures IDs are strings of four characters, a mix of uppercase letters and digits. To avoid selecting similar strings, I use a negative look-ahead regular expression.

Since the arrayexpress index typically return x100,000 genes, a filtering step happens to query the uniprot index only with genes that are known to be stored in it. This is because there is currently a small overlap between the two indices, so a lot of computation time would be spent looking for records which are not there. To prevent this, I generate a set of documents ID from the uniprot index, which is used to filter the results from the arrayexpress index. This speeds-up queries by a factor of two.

## Improvements

The current organization of the Elasticsearch database was designed to normalize it and avoid redundancy. This forces us to perform joins between different indices, which is time consuming, especially since there is a low overlap between the first and second index. I am investigating a different organization of the database to overcome low overlap between indices. Instead of filtering intermediate results of searching the first index with documents ID from the second index, I could rebuild a filtered version of the first index.

Another approach to organize the Elasticsearch database would integrate data currently spread between indices into different fields of the same document. This would increase a lot the redundancy of the database, since many documents from one index relate to a single document of another index, but would increase query speed.

I used Spark because my data are not streaming (the data in AWS S3 are updated weekly) and because it provides fast processing as long as the data fit in memory. ArrayExpress data represent 4TB, so I have to process chunks of data so they can fit in memory. MapReduce is an alternative to Spark, and may outperform Spark when processing the ArrayExpress dataset, given its size, I will benchmark the two batch processing tools.

## Repository
```
budgies/
  |- src
  | |- ingestion/
  | | |- arrayexpress_experiments.py
  | | |- arrayexpress_files.py
  | | |- download_arrayexpress.sh
  | | |- uniprot_files.py
  | | |- get_molecules.py
  | | |- headers.json
  | |- spark/
  | | |- arrayexpress_spark_es.py
  | | |- sort_s3_folder.py
  | | |- sort_s3_folder.sh
  | | |- uniprot_to_txt_file.py
  | | |- txt_file_to_es.py
  | | |- spark_uniprot_es.sh
  | | |- genes_molecules.py
  | |- config.txt
  |- webui/
  | |- output/
  | |- static/
  | | |- css/
  | | |- fonts/
  | | |- js/
  | |- templates/
  | | |- base.html
  | | |- query.html
  | | |- response.html
  | |- __init__.py
  | |- view.py
  | |- query_db.py
  |- tools/
  | |- unique_geneids.py
  | |- unique_pdbids.py
  |- test/
  | |- test_new_accessions_2/
  | | |- ref_exp-20131231.json
  | | |- ref_exp-20140101.json
  | |- test_arrayexpress_files.py
  | |- ref_exp.json
  |- docs/
  |- run.py
  |- README.md
```
## Support

For any question, send an email to budgies.results@gmail.com.
