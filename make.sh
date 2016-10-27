#!/usr/bin/env bash

#Change the paths here
WORKFLOWSBASE=~/github/dig/dig-workflows


# This script needs to change only if we add new non installed packages
CURRENT=`pwd`
rm -rf lib
mkdir lib
cd lib
LIB_FOLDER=`pwd`
conda create -m -p $LIB_FOLDER/effect-env --copy --clone effect-env
mkdir tozip
cp -rf effect-env/lib/python2.7/site-packages/* tozip/
rm -rf effect-env

cp ../*.py tozip/
cp ../scripts/APIDownloader/*.py tozip/
cd tozip

mkdir digWorkflow
cp $WORKFLOWSBASE/pySpark-workflows/digWorkflow/*  ./digWorkflow/
zip -r python-lib.zip *
mv python-lib.zip ../
cd ..
rm -rf tozip
cd ..