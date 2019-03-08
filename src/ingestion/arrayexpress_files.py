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
from subprocess import run

def print_help():
    """Print help."""

    help_text = \
"""Download files corresponding to a list of experiments from ArrayExpress\
then copy them to an AWS S3 bucket.

    python3 arrayexpress_files.py -d[working directory] -h

    Please provide an option for the working directory and the S3 bucket.

    -d, --directory
    Full path to the working directory.

    -b, --bucket
    Name of the AWS S3 bucket.

    -h, --help
    Display help."""

    print(help_text)

def get_args():
    """Get arguments passed when the script is run at the command line."""

    try:
        opts, args = getopt.getopt(sys.argv[1:],
                'd:b:h',
                ['directory=', 'bucket=', 'help'])

    except getopt.GetoptError as e:
        print(e)
        sys.exit(2)

    if len(args) > 0:
        print("""This script does not take arguments outside options.
        Please make sure you did not forget to include an option name.""")

    work_dir = None
    bucket = None

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_help()
            sys.exit()
        elif opt in ('-d', '--directory'):
            work_dir = arg
        elif opt in ('-b', '--bucket'):
            bucket = arg

    return work_dir, bucket

def log_error(err_msg, work_dir):
    """Log error in a text file."""
    
    # if the script is in an accession-specific directory :
    # i.e. not in the working directory
    # get current directory, save log in working directory
    if not os.getcwd() == work_dir:
        curr_dir = os.getcwd()
        os.chdir(work_dir)
  
        with open('log_arrayexpress_files.txt', 'a') as log:
            log.write(str(datetime.now()) + ' ')
            log.write(err_msg + '\n')
        
        # go back to previous directory
        os.chdir(curr_dir)

    else:
        with open('log_arrayexpress_files.txt', 'a') as log:
            log.write(str(datetime.now()) + ' ')
            log.write(err_msg + '\n')

def all_accessions(file_name):
    """Return a list of all experiments accession from an experiment.json file."""

    # load json file
    with open(file_name, 'r') as json_file:
        exp = json.load(json_file)

    # number of experiments
    n_exp = exp['experiments']['total']

    # return a list of accession names
    acc = [''] * n_exp
    for i in range(n_exp):
        acc[i] = exp['experiments']['experiment'][i]['accession']

    return acc

def new_accessions():
    """Return a list of new accessions by comparing the last two experiments.json files."""

    # list of two most recent files names
    last_files = sorted([f for f in os.listdir(os.getcwd()) if f.endswith('.json')])[-2:]

    # check if there is any file to be processed
    if not last_files:
        print('There is no experiments.json files to be processed')
        sys.exit()

    # return all accessions if there is only one file
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
    
    # else there are more than two experiments.json files
    # return the most recent accessions
    # two most recent files contents
    else:
        last_exp = [0] * 2
        for i in range(2):
            with open(last_files[i], 'r') as json_file:
                last_exp[i] = json.load(json_file)

        # list of experiments number for the two recent files
        n_exp = [0] * 2
        for i in range(2):
            n_exp[i] = last_exp[i]['experiments']['total']

        # if number accessions is the same in the two files, return empty list
        if n_exp[0] == n_exp[1]:
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
    if file_url.split('.')[-1] == 'txt':
        try:
            response = requests.get(file_url, headers=headers, timeout=timeout)
            if response.ok:

                # get file name as last part of the url
                file_name = file_url.split('/')[-1]
                with open(file_name, 'w') as outfile:
                    outfile.write(response.text)

            else:
                err_msg = 'An error occured when trying to get {0}\n\
The response from the server was {1}'\
                        .format(file_url, response.status_code)
                print(err_msg)
                log_error(err_msg, work_dir)
    
        except requests.ConnectionError as e:
            err_msg = 'Connection error when trying to get {0}\n{1}'\
                    .format(file_url, str(e))
            print(err_msg)
            log_error(err_msg, work_dir)

        except requests.Timeout as e:
            err_msg = 'Timeout when trying to get {0}\n{1}'\
                    .format(file_url, str(e))
            print(err_msg)
            log_error(err_msg, work_dir)

        except requests.RequestException as e:
            err_msg = 'General error when trying to get {0}\n{1}'\
                    .format(file_url, str(e))
            print(err_msg)
            log_error(err_msg, work_dir)
    
    # for a zip file
    elif file_url[-3:] == 'zip':
        try:
            response = requests.get(file_url, headers=headers, timeout=timeout, stream=True)
            if response.ok:
                try:
                    z = zipfile.ZipFile(io.BytesIO(response.content))
                    z.extractall()
                except zipfile.BadZipfile as e:
                    file_name = file_url.split('/')[-1]
                    err_msg = 'Error when trying to unzip {0}\n{1}'\
                            .format(err_msg, e)
                    print(err_msg)
                    log_error(err_msg, work_dir)

        except requests.ConnectionError as e:
            err_msg = 'Connection error when trying to get {0}\n{1}'\
                    .format(file_url, str(e))
            print(err_msg)
            log_error(err_msg, work_dir)

        except requests.Timeout as e:
            err_msg = 'Timeout when trying to get {0}\n{1}'\
                    .format(file_url, str(e))
            print(err_msg)
            log_error(err_msg, work_dir)

        except requests.RequestException as e:
            err_msg = 'General error when trying to get {0}\n{1}'\
                    .format(file_url, str(e))
            print(err_msg)
            log_error(err_msg, work_dir)


def get_accession_files(url_prefix, accession, headers, timeout):
    """
    Download the json file containing the list of files for a specific accession \
and return a list of URLs for files from a specific accession number.
    """
    # URL of json file for a specific accession
    url = url_prefix + accession

    # get json file
    try:
        response = requests.get(url, headers=headers, timeout=timeout)

        # if request was successful
        if response.ok:

            # save file
            file_name = '{0}.json'.format(acc)
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
The response from the server was {1}'\
                    .format(url, response.status_code)
            print(err_msg)
            log_error(err_msg, work_dir)

    except requests.ConnectionError as e:
        err_msg = 'Connection error when trying to get {0}\n{1}'\
                .format(url, str(e))
        print(err_msg)
        log_error(err_msg, work_dir)

    except requests.Timeout as e:
        err_msg = 'Timeout when trying to get {0}\n{1}'\
                .format(url, str(e))
        print(err_msg)
        log_error(err_msg, work_dir)

    except requests.RequestException as e:
        err_msg = 'General error when trying to get {0}\n{1}'\
                .format(url, str(e))
        print(err_msg)
        log_error(err_msg, work_dir)


if __name__ == '__main__':
    # get URL prefix for the API
    url_prefix = 'https://www.ebi.ac.uk/arrayexpress/json/v3/files/'

    # import headers for API query
    with open('headers.json', 'r') as infile:
        headers = json.load(infile)

    # get arguments from script call
    work_dir, bucket_suffix = get_args()
    bucket = 's3://' + bucket_suffix

    # go to working directory
    os.chdir(work_dir)

    # get accessions that need to be processed
    print('Getting list of experiment accessions to process...')
    accessions = new_accessions()
    print('{0} new accessions to process.'.format(len(accessions)))

    # loop over each accession
    for acc in accessions:

        print('Processing accession {0}'.format(acc))

        # create a directory for each accession
        try:
            os.makedirs(acc)
            os.chdir(acc)

        # if the directory already exists, delete its contents
        except FileExistsError:
            print('The directory "{0}" already exists.'.format(acc))
            os.chdir(acc)
            path = os.getcwd()
            for f in os.listdir(path):
                file_path = os.path.join(path, f)
                os.remove(file_path)
            print('Removed directory "{0}" and its contents.'.format(acc))
    
        # get list of files (containing experimental results) to downloads
        files_url = get_accession_files(url_prefix, acc, headers, 300)

        # download each file
        print('Downloading files...')
        for url in files_url:
            download_file(url, headers, 300)

        # go back to working directory
        os.chdir('..')

        # copy directory to S3 bucket by calling a bash command
        bucket_dir = bucket + '/' + acc
        process = run(['aws', 's3', 'cp', acc, bucket_dir, '--recursive'])

        # delete the directory if copy to S3 bucket copy was successful
        if process.returncode == 0:
            # loop through each file name in the directory and delete them
            for f in os.listdir(acc):
                os.remove(os.path.join(acc, f))
            os.rmdir(acc)

        print('Processed accession {0}.'.format(acc))

    print('Finished downloading new accession files')
