rm bin/TermDocIndexer.jar
javac -classpath $HADOOP_HOME/hadoop-core-0.20.203.0.jar -d TermDocIndexer/ TermDocIndexer/org/myorg/TermDocIndexer.java TermDocIndexer/org/myorg/IntArrayWritable.java 
jar -cvf bin/TermDocIndexer.jar -C TermDocIndexer/ .
