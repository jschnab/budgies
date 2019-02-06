# script which runs a query on Budgies databases

PROJECT=$1
QUERY=$2

cd ~/budgies/src

python3 query_db.py -q $QUERY

cd output

tar -czvf results.tar.gz experiment_description.txt molecules.csv

aws s3 cp results.tar.gz s3://budgies-results/$PROJECT/

rm results.tar.gz

cd ..
