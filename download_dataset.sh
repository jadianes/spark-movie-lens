#!/usr/bin/env bash

hash wget 2>/dev/null || { echo >&2 "Wget required.  Aborting."; exit 1; }
hash unzip 2>/dev/null || { echo >&2 "unzip required.  Aborting."; exit 1; }

wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip -o "ml-latest.zip"
unzip -o "ml-latest-small.zip"
DESTINATION="./datasets/"
mkdir -p $DESTINATION
mv ml-latest $DESTINATION
mv ml-latest-small $DESTINATION