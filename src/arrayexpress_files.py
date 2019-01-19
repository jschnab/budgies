#!bin/python3

# script which downloads files from arrayexpress from a list of experiments

import requests
import json
import getopt
import sys
import os
from datetime import datetime
import zipfile
import io

def print_help():
    """Print help."""

    help_text = \
"""Download files corresponding to a list of experiments from ArrayExpress.

    python3 arrayexpress_files.py -d[working directory] -h

    Please provide an option for the working directory.

    -d, --directory
    Full path to the working directory.

    -h, --help
    Display help."""

    print(help_text)

def get_args():
    """Get arguments passed when the script is run at the command line."""

    try:
        opts, args = getopt(sys.argv[1:],
                'd:h',
                ['directory=', 'help'])

    except getopt.GetoptError as e:
        print(e)
        sys.exit(2)

    if len(args) > 0:
        print("""This script does not take arguments outside options.
        Please make sure you did not forget to include an option name.""")

    work_dir = None

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_help()
            sys.exit()
        elif opt in ('-d', '--directory'):
            work_dir = arg

    return work_dir

def log_error(err_msg, work_dir):
    """Log error in a text file."""
    
    # if the script is in an accession-specific directory :
    # get current directory, save log in working directory
    if not os.getcwd() == work_dir:
        curr_dir = os.getcwd()
        os.chdir(work_dir)
  
        with open('log_arrayexpress_files.txt', 'a') as log:
            log.write(str(datetime.now()) + ' ')
            log.write(err_msg)
        
        # go back to previous directory
        os.chdir(curr_dir)

    else:
        with open('log_arrayexpress_files.txt', 'a') as log:
            log.write(str(datetime.now()) + ' ')
            log.write(err_msg)

def new_accessions():
    """Return a list of new accessions by comparing the last two experiments.json files."""

    # list of two most recent files names
    last_files = sorted([f for f in os.listdir(os.getcwd()) if f.endswidth('.json')])[-2:]

    # return all accession if there is only one file
    if len(last_files) == 1:

        # load json file
        with open(last_files[0], 'r') as json_file:
            last_exp = json.load(json_file)
        
        # make a list of all experiments
        n_exp = last_exp['experiments']['total']

        # return list of all accessions names
        acc = [''] * n_exp
        for i in range(n_exp):
            acc[i] = last_exp['experiments']['experiment'][i]['accession']

        return acc
    
    # two most recent files contents
    last_exp = [0] * 2
    for i in range(2):
        with open(last_files[i], 'rb') as json_file:
            last_exp[i] = json.load(json_file)

    # list of experiments number for the two recent files
    n_exp = [0] * 2
    for i in range(2):
        n_exp[i] = last_exp[i]['experiments']['total']

    # if no new accessions, return empty list
    if n_exp[0] = n_exp[1]:
        return []

    # else return list of new accession names
    else:
        # number of new accessions
        n_new = n_exp[1] - n_exp[0]

        # initialize empty list to contain new accessions
        new_acc = [''] *  n_new
        for i in range(n_new):
            new_acc[i] = last_exp[1]['experiments']['experiment'][i]['accession']

        return new_acc

def download_file(file_url, headers, timeout):
    """Download a file containing experimental results."""

    # for a text file
    if file_url[-3:] == 'txt':
        try:
            response = requests.get(file_url, headers=headers, timeout=timeout)
            if response.ok:

                # get file name as last part of the url
                file_name = file_url.split('/')[-1]
                with open(file_name) as outfile:
                    outfile.write(response.text)

            else:
                err_msg = 'An error occured when trying to get {0}\n\
                        The response from the server was{1}'\
                        .format(file_url, response.status_code)
                print(err_msg)
                log_error(err_msg, work_dir)
    
    # for a zip file
    elif file_url[-3] == 'zip':
        try:
            response = requests.get(file_url, headers=headers, timeout=timeout, stream=True)
            if response.ok:
                z = zipfile.ZipFile(io.BytesIO(response.content))
                z.extractall()

def get_accession_files(url_prefix, accession, headers, timeout):
    """
    Download the json file containing the list of files for a specific accession \
and return a list of URLs for files from a specific accession number.
    """
    # URL of json file for a specific accession
    url = url_prefix + 'files/' + accession

    # get json file
    response = requests.get(url, headers=headers, timeout=timeout)
    if response.ok:

        # save file
        with open(file_name, 'wb') as outfile:
            outfile.write(response.content)

        # make dictionary out of json file
        files_dict = json.loads(response.content)

        # get number of files
        n_files = len(files_dict['files']['experiment']['file'])

        # initialize empty list of files URL and
        # loop over files to get URLs
        files_url = [''] * n_files
        for i in range(n_files):
            files_url[i] = files_dict['files']['experiment']['file'][i]['url']

        return files_url

    # if request was unsuccessful
    else:
        err_msg = 'An error occured when trying to get {0}\n\
                The response from the server was{1}'\
                .format(file_url, response.status_code)
        print(err_msg)
        log_error(err_msg, work_dir)

# get URL prefix for the API
url_prefix = 'https://www.ebi.ac.uk/arrayexpress/json/v3/'

# import headers for API query
with open('headers.json', 'r') as infile:
    headers = json.load(infile)

# get arguments from script call
work_dir = get_args()

# get accessions that need to be processed
accessions = new_accessions()

# loop over each accession
for acc in accessions:

    # create a directory for each accession
    os.makedirs(acc)
    os.chdir(acc)
    
    # get list of files (containing experimental results) to downloads
    files_url = get_accession_files(url_prefix, acc, headers, 20)

    # download each file
    for url in files_url:
        download_file(url, headers, 20)

    # go back to working directory
    os.chdir('..')
