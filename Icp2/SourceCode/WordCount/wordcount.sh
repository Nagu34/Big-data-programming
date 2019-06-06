#!/bin/bash
 
mkdir -p build
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* WordCount.java -d build -Xlint 
jar -cvf wordcount.jar -C build/ . 
hadoop jar wordcount.jar com.wordcount.WordCount /user/cloudera/naxbergo/wordcount/input /user/cloudera/naxbergo/wordcount/output
hadoop fs -cat /user/cloudera/naxbergo/wordcount/output/*
