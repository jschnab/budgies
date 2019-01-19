#!bin/python3

# script which downloads list of experiments from the ArrayExpress database through the API

import requests
import json
import getopt
import sys
import os
from datetime import datetime

def print_help():
    """Print help."""

    help_text = \
"""Download list of experiments from ArrayExpress and return it as a json file.

    python3 arrayexpress_experiments.py -o[output directory] -s[search suffix] -h

    Please provide options for output directory and search suffix.

    -o, --output-dir
    Full path to the directory to put downloads.

    -s, --search-suffix
    Suffix for the URL to search ArrayExpress.
    & must be escaped with \

    -h, --help
    Display help."""

    print(help_text)

def get_args():
    """Get arguments passed when the script is run at the command line."""

    try:
        opts, args = getopt.getopt(sys.argv[1:],
                'o:s:h',
                ['output-dir=', 'search-suffix=', 'help'])

    except getopt.GetoptError as e:
        print(e)
        sys.exit(2)

    if len(args) > 0:
        print("""This script does not take arguments outside options.
        Please make sure you did not forget to include an option name.""")

    output_dir = None
    search_suffix = None

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print_help()
            sys.exit()
        elif opt in ('-o', '--output-dir'):
            output_dir = arg
        elif opt in ('-s', '--search-suffix'):
            search_suffix = arg

    if output_dir == None:
        print('Please provide an output directory.')
        sys.exit()

    if search_suffix == None:
        print('Please provide a search.')
        sys.exit()

    return output_dir, search_suffix

def log_error(err_msg):
    """Log error in a text file."""
    with open('log_arrayexpress_exp.txt', 'a') as log:
        log.write(str(datetime.now()) + ' ')
        log.write(err_msg)

def get_experiments(search_url, headers, timeout):
    """Return the json file containing experiments corresponding to a specific search."""

    # get response from ArrayExpress
    try:
        response = requests.get(search_url, headers=headers, timeout=timeout)
        
        # if request was successful
        if response.ok:
            return json.loads(response.content)

        # if request not successful display and log error
        else:
            err_msg = 'An error occurred when trying to get {0}\n\
                    The response from the server was {1}'\
                    .format(search_url, response.status_code)
            print(err_msg)
            log_error(err_msg)

    except requests.ConnectionError as e:
        err_msg = 'Connection error when trying to get {0}\n{1}'\
                .format(search_url, str(e))
        print(err_msg)
        log_error(err_msg)

    except requests.Timeout as e:
        err_msg = 'Timeout when trying to get {0}\n{1}'\
                .format(search_url, str(e))
        print(err_msg)
        log_error(err_msg)

    except requests.RequestException as e:
        err_msg = 'General error when trying to get {0}\n{1}'\
                .format(search_url, str(e))
        print(err_msg)
        log_error(err_msg)

# get arguments from script call
output_dir, search_suffix = get_args()

# import headers for ArrayExpress API
with open('headers.json', 'r') as infile:
    headers = json.load(infile)
    
# create output directory if it does not exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
os.chdir(output_dir)
print('\nWorking in {0}'.format(output_dir))

# prefix url for searching experiments
search_prefix = 'https://www.ebi.ac.uk/arrayexpress/json/v3/experiments?'

# search url
search_url = search_prefix + search_suffix

# download experiments list
print('Downloading experiments from {0}'.format(search_url))
experiments = get_experiments(search_url, headers, 20)

# saving experiments as json file if download was successful
if experiments is not None:
    print('Saving data in "experiments.json"')
    with open('experiments.json', 'w') as json_file:
        json.dump(experiments, json_file)

