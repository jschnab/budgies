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

def log_error(err_msg):
    """Log error in a text file."""
    with open('log_arrayexpress_files.txt', 'a') as log:
        log.write(str(datetime.now()) + ' ')
        log.write(err_msg)

def download_file(file_url, headers, timeout):
    """Download a file."""

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
                log_error(err_msg)
    
    # for a zip file
    elif file_url[-3] == 'zip':
        try:
            response = requests.get(file_url, headers=headers, timeout=timeout, stream=True)
            if response.ok:
                z = zipfile.ZipFile(io.BytesIO(response.content))
                z.extractall()

def get_accession_files(experiments):
    """Get URLs for files of a single experiment accession."""

# get total number of experiments
n_exp = experiments['experiments']['total']
