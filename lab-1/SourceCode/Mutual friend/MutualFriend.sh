mkdir -p build
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* MutualFriend.java -d build -Xlint 
jar -cvf MutualFriend.jar -C build/ . 
hadoop jar MutualFriend.jar MutualFriend /user/cloudera/naxbergo/MutualFriend/input /user/cloudera/naxbergo/MutualFriend/output1
hadoop fs -cat /user/cloudera/naxbergo/MutualFriend/output1/*

