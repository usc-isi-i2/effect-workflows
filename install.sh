#!/bin/sh
CUR_DIR="$PWD"

/bin/bash ./clean.sh
cd ..
git clone https://github.com/usc-isi-i2/dig-workflows.git
cd dig-workflows
git pull

WORKFLOWSBASE="$PWD"

cd $CUR_DIR
echo "Updating make.sh.."
grep -v "WORKFLOWSBASE=" make.sh > make2.sh
echo WORKFLOWSBASE=$WORKFLOWSBASE > make.sh
cat make2.sh >> make.sh
rm make2.sh

echo "Running make.sh..."
conda env create -f environment.yml
/bin/bash ./make.sh

echo "Downloading Karma"
cd jars
curl -s https://api.github.com/repos/usc-isi-i2/Web-Karma/releases/latest > karma-latest.json
grep '"tag_name":' karma-latest.json | sed -e 's/"tag_name"://g' | sed -e 's/"//g' | sed -e 's/,//g' | sed -e 's/ //g' > karma-version.txt
KARMA_VERSION=`cat karma-version.txt`
wget https://github.com/usc-isi-i2/Web-Karma/releases/download/$KARMA_VERSION/karma-spark-0.0.1-SNAPSHOT-1.6.0-cdh5.10.1-hive.jar
cd ../..
git clone https://github.com/usc-isi-i2/dig-alignment.git
cd dig-alignment
git checkout development
git pull
cd versions/3.0/
zip -r karma.zip karma
cp karma.zip $CUR_DIR/
cd $CUR_DIR

echo "Copying all files to hdfs..."
hdfs dfs -mkdir /user/effect
hdfs dfs -mkdir /user/effect/workflow
hdfs dfs -mkdir /user/effect/workflow/lib
hdfs dfs -mkdir /user/effect/data
hdfs dfs -mkdir /user/effect/data/hive-backup
hdfs dfs -mkdir /user/effect/data/karma-out

hdfs dfs -put -f python-lib.zip /user/effect/workflow/lib/
hdfs dfs -put -f effect-env.zip /user/effect/workflow/lib/
hdfs dfs -put -f pyspark /user/effect/workflow/lib/
hdfs dfs -put -f jars/elasticsearch-hadoop-2.4.0.jar /user/effect/workflow/lib/
hdfs dfs -put -f jars/karma-spark-0.0.1-SNAPSHOT-1.6.0-cdh5.10.1-hive.jar /user/effect/workflow/lib/
hdfs dfs -put -f karma.zip /user/effect/workflow/lib/
hdfs dfs -put -f scripts/APIDownloader/*.py /user/effect/workflow/
hdfs dfs -put -f effectWorkflow.py /user/effect/workflow/
hdfs dfs -put -f effectWorkflow-es.py /user/effect/workflow/
hdfs dfs -put -f ransomware-workflow.py /user/effect/workflow/
hdfs dfs -put -f sparkRunCommands/*.sh /user/effect/workflow/

echo "DONE"