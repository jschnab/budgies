#!bin/bash

# script which downloads experiments.json files from ArrayExpress year by year
# since downloading all years results in timeout

YEAR=2019
while [[ ${YEAR} -gt 2000 ]]; do
  python3 arrayexpress_experiments.py -o /home/ec2-user/arrayexpress -s species=\"homo+sapiens\"\&processed=true\&date=${YEAR}*
  let YEAR=${YEAR}-1
done
