#!/bin/sh

docker compose build db

docker compose build initdb

docker compose build webapi

wget --no-remove-listing --no-passive-ftp ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/
awk '{print $9}' .listing | tr -d '\r' | parallel 'aria2c -k 1048576 -s 16 --file-allocation=none -j 16 -x 16 --continue=true http://ftp.ncbi.nlm.nih.gov/pubmed/baseline/{}'
wget http://ftp.ebi.ac.uk/pub/databases/genenames/new/tsv/hgnc_complete_set.txt
wget http://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_info.gz
wget http://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2pubmed.gz
wget http://ftp.ncbi.nlm.nih.gov/pub/lu/PubTator3/gene2pubtator3.gz

ls -1 *.gz | parallel 'gunzip {}'

docker compose up -d db
sleep 30
docker compose up initdb

docker compose wait initdb

docker compose up -d webapi
