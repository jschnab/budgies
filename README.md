# Quick as a Batch (aka budgies) - Facilitating drug discovery through databases integration

## Table of Contents
1. [Introduction](README.md##introduction)
2. [Motivation](README.md##motivation)
3. [Approach](README.md##approach)
4. [Pipeline](README.md##pipleline)
5. [Challenges](README.md##challenges)
6. [Repository structure](README.md##repository structure)
7. [Questions?](README.md##questions?)

## Introduction

Quick as a Batch is a data analysis tool which aims at facilitating drug discovery by integrating biological databases with Apache Spark. It provides a web interface built with Flask to query an Elasticsearch database and provides fast results. This project was achieved in three weeks while I was a fellow at Insight Data Science, a company helping people with skills in software engineering to transition to data engineering.

Please read my [demonstration](https://bit.ly/2BotnFY)

## Motivation

There is a need for drug discovery. Drug performance is impaired by antibiotic resistance in micro-organisms, drug resistance in cancer cells, etc. Also, less than 1% of drug candidates will actually prove safe and able to cure, so pharmaceutical companies need to screen thousands of drugs. Moreover, technical advances in biology have vastly increased the breadth of available data, but there is poor integration between datasets. We can increase the value of these data by uncovering how they related.

## Approach

To solve the aforementioned issues, Quick as a Batch merges gene expression data from the ArrayExpress database with protein structure data from the Protein Data Bank (PDB). The link between the two is provided by the Uniprot database, which can be viewed as an index containing references to genes and proteins. From ArrayExpress, I extract description of experiments and a list of genes resulting from the experiment. Typically, a user would query the experiment descriptions for keywords (e.g. a disease name) and obtain a gene list. This gene list is converted into a protein list thanks to Uniprot. Finally, the protein list is used to query PDB which results in protein structures containing molecules. A data scientist can then use the list of molecules to design new drugs.

## Pipeline

The data is ingested from the external databases thanks to scripts automating API queries. Data from ArrayExpress (1 TB) and Uniprot (1 GB) is stored in Amazon Web Services Simple Storage System (AWS S3). These data represent a fraction of the original databases corresponding only to data relevant to human (many more species are available). Data from the PDB is downloaded directly to Elasticsearch with the help of the PyPDB Python library, since data volume is more limited. All the data of interest are text files.

Spark is used to parse the text files and extract useful information from them (list of genes, of proteins, etc.). The output is dictionaries which are serialized for storage in Elasticsearch.

Elasticsearch contains three indices: arrayexpress (genes), uniprot (proteins) and molecules. When a user sends a query to Quick as a Batch, keywords are used to query arrayexpress on experiment descriptions. Matches give a gene list which is used to query the uniprot index. However, since the arrayexpress index typically return x100,000 genes, a filtering step happens to query the uniprot index only with genes that are known to be stored in it. This is because there is currently a small overlap between the two indices, so a lot of computation time would be spent looking for records which are not there. Finally, the results returned by the uniprot index are used to query the molecules index, yielding a list of molecules. There is 100% overlap between uniprot and molecules indices.
