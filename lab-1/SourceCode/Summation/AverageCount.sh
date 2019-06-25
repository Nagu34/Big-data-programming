mkdir -p build
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint 
jar -cvf AverageCount.jar -C build/ . 
hadoop jar AverageCount.jar AverageCount /user/cloudera/naxbergo/AverageCount/input /user/cloudera/naxbergo/AverageCount/output
hadoop fs -cat /user/cloudera/naxbergo/AverageCount/output/*

