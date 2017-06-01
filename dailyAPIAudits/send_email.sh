#!/bin/bash

irregularities_path="/user/hive/warehouse/effect_daily_irregularitites"
smtpServer="smtp.isi.edu"
fromMailID="dipsy@isi.edu"
toMailIDs="dipsykapoor@gmail.com"
subject="EFFECT DAILT AUDIT EMAIL"
body="Please find below the sources with probable inconsistencies in the number of records:"
team="ISI" #ISI

records=`hdfs dfs -cat $irregularities_path/* | wc -l`
echo "Got $records"

if [ $records > 0 ]; then

	if [ -f body.txt ]; then
	   rm body.txt
	fi

	echo $body > body.txt
	echo $'\n' >> body.txt

	hdfs dfs -cat $irregularities_path/* >> body.dat
	echo $'\n' >> body.txt

	echo $team >> body.txt
	mail -S smtp=$smtpServer -r $fromMailID -s $subject -v $toMailIDs < body.txt

fi
