# script which downloads files corresponding to accessions retrieved
# from a uniprot query

import requests
import json
import getopt
import sys
import os
from datetime import datetime
import time
from subprocess import run

def print_help():
    """Print help."""

    help_text = \
"""Download files corresponding to Uniprot Entry IDs retrieved from a Uniprot search \
and transfer them to an AWS S3 bucket.

    python3 uniprot_files.py -d[working directory] -s[search suffix] -b[bucket name] -h

    Please provide path to the working directory, the search suffix and the bucket name.

    -d, --directory
    Full path to the directory to put downloads

    -s, --search
    Suffix for the URL to search Uniprot.

    -b, --bucket
    Name of the AWS S3 bucket.

    -h, --help
    Display help."""

    print(help_text)

def get_args():
    """Get arguments passed when the script is run at the command line."""

    try:
        opts, args = getopt.getopt(sys.argv[1:],
                'd:s:b:h',
                ['directory=', 'search=', 'bucket=', 'help'])

    except getopt.GetoptError as e:
        print(e)
        sys.exit(2)

    if len(args) > 0:
        print("""This script does not take arguments outside options.
Please make sure you did not forget to include an option name.""")

    directory = None
    search = None
    bucket = None

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_help()
            sys.exit()
        elif opt in ('-d', '--directory'):
            directory = arg
        elif opt in ('-s', '--search'):
            search = arg
        elif opt in ('-b', '--bucket'):
            bucket = arg

    if directory == None:
        print('Please provide a directory path.')
        sys.exit()

    if search == None:
        print('Please provide a search suffix.')
        sys.exit()

    if bucket == None:
        print('Please provide an AWS S3 bucket.')
        sys.exit()

    return directory, search, bucket

def log_error(err_msg):
    """Log error in a text file."""
    with open('log_uniprot.txt', 'a') as log:
        log.write(str(datetime.now()) + ' ')
        log.write(err_msg + '\n')

def search_uniprot(search_suffix, headers):
    """Download the text file given by a Uniprot search and \
return list of entries from the text file."""

    # build full URL by joining search prefix and suffix
    search_prefix = 'https://www.uniprot.org/uniprot/?query='
    url = search_prefix + search_suffix
    
    # get response from Uniprot
    try:
        response = requests.get(url, headers=headers, timeout=300)

        if response.ok:

            # save search as text file
            today = datetime.today().strftime('%Y%m%d')
            file_name = 'uniprot_entries-{0}.txt'.format(today)
            with open(file_name, 'w') as outfile:
                outfile.write(response.text)

            search_results = response.text.split('\n')
            
            # generate list of Uniprot entries
            # last line is empty
            uniprot_entries = [''] * (len(search_results) - 1)
            for i in range(len(search_results) - 1):
                # first column contains uniprot entries
                uniprot_entries[i] = search_results[i].split('\t')[0]

            # discard first element (column name)
            return uniprot_entries[1:]

        else:
            err_msg = 'Error when trying to get {0}\n\
The status code from the server was {1}.'.format(url, response.status_code)
            print(err_msg)
            log_error(err_msg)

    except requests.ConnectionError as e:
        err_msg = 'Connection error when trying to get {0}\n{1}'\
                .format(url, str(e))
        print(err_msg)
        log_error(err_msg)

    except requests.Timeout as e:
        err_msg = 'Timeout when trying to get {0}\n{1}'\
                .format(url, str(e))
        print(err_msg)
        log_error(err_msg)

    except requests.RequestException as e:
        err_msg = 'General error when trying to get {0}\n{1}'\
                .format(url, str(e))
        print(err_msg)
        log_error(err_msg)

def download_entry(entry, headers):
    """Download the text file corresponding to a Uniprot entry."""
    
    # build full URL by joining search prefix and suffix
    url = 'https://www.uniprot.org/uniprot/{0}.txt'.format(entry)
    
    # get response from Uniprot
    try:
        response = requests.get(url, headers=headers, timeout=20)

        if response.ok:

            # save search as text file
            file_name = '{0}.txt'.format(entry)
            with open(file_name, 'w') as outfile:
                outfile.write(response.text)

            return file_name

        else:
            err_msg = 'Error when trying to get {0}\n\
The status code from the server was {1}.'.format(url, response.status_code)
            print(err_msg)
            log_error(err_msg)

    except requests.ConnectionError as e:
        err_msg = 'Connection error when trying to get {0}\n{1}'\
                .format(url, str(e))
        print(err_msg)
        log_error(err_msg)

    except requests.Timeout as e:
        err_msg = 'Timeout when trying to get {0}\n{1}'\
                .format(url, str(e))
        print(err_msg)
        log_error(err_msg)

    except requests.RequestException as e:
        err_msg = 'General error when trying to get {0}\n{1}'\
                .format(url, str(e))
        print(err_msg)
        log_error(err_msg)

def copy_to_s3(entry_file, bucket):
    """Copy the text file corresponding to a Uniprot entry to and AWS S3 bucket."""

    # destination file
    dest_file = bucket + '/' + entry_file

    # copy entry file to S3
    process = run(['aws', 's3', 'cp', entry_file, dest_file])
        
    # check if copy to AWS S3 was successful
    if process.returncode  == 0:
        # delete the entry file
        os.remove(file_name)

    else:
        print('Could not copy {0} to {1}'.format(file_name, bucket))

if __name__ == '__main__':
    
    # get headers for requests.get()
    with open('headers.json', 'r') as infile:
        headers = json.load(infile)

    # get arguments from command call
    work_dir, search_suffix, bucket_suffix = get_args()
    bucket = 's3://budgies/' + bucket_suffix

    # check if working directory exists
    # and make it if it does not exist
    if not os.path.exists(work_dir):
        os.makedirs(work_dir)

    # go to the working directory
    os.chdir(work_dir)
    print('Working in {0}'.format(work_dir))

    # get list of Uniprot entries
    # and save as text file
    uniprot_entries = search_uniprot(search_suffix, headers)

    # process entries if search was successful
    if uniprot_entries is not None:
        print('Retrieved {0} entries.'.format(len(uniprot_entries)))
        # loop over each individual entry
        print('Downloading files for each Uniprot entry...')
        for entry in uniprot_entries:

            # download text file for each Uniprot entry
            file_name = download_entry(entry, headers)

            # copy file to AWS S3
            copy_to_s3(file_name, bucket)
