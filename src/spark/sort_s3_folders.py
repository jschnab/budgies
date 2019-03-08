# script which reads a text file containing folder names and another
# containing their size, then saves a text file with folder names sorted by size

import os

# get path of the file containing accessions (= folders) names
with open('config.txt', 'r') as config:
    while True:
        line = config.readline()
        if not line:
            break
        else:
            splitted = line.split('=')
            if splitted[0] == 'accession_file':
                accessions = splitted[1].strip()

# make a list from the text file containing folder names
folders = []
with open(accessions, 'r') as infile:
    while True:
        line = infile.readline().strip()
        if not line:
            break
        else:
            folders.append(line)

# make a list from the text file containing folder sizes
with open('sizes.txt', 'r') as infile:
    while True:
        line = infile.readline().strip()
        if not line:
            break
        else:
            sizes = [float(s) for s in line.split()]

# get index of sorted sizes then sort folders
sort_index = sorted(range(len(sizes)), key=lambda i: sizes[i])
sorted_folders = [folders[i] for i in sort_index]

# save result in the same format as the original text file
with open('sorted_folders.txt', 'w') as outfile:
    for sf in sorted_folders:
        outfile.write(sf + '\n')
