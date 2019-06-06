#!/bin/bash
 
mkdir -p build
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* WordCount.java -d build -Xlint 
jar -cvf wordcount.jar -C build/ . 

echo "Executing the program & Running the job"
echo "-------------------------------------------------------------------"
hadoop jar wordcount.jar com.wordcount.WordCount /user/cloudera/naxbergo/oddcount/input /user/cloudera/naxbergo/oddcount/output1
echo"---------------------------------------------------------------------------"
echo "output"
echo"----------------------------------------------------------------------------"
hadoop fs -cat /user/cloudera/naxbergo/oddcount/output1/*

