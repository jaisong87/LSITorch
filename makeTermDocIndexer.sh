rm bin/TermDocIndexer.jar
javac -classpath $HADOOP_HOME/hadoop-0.20.2-core.jar -d TermDocIndexer/ TermDocIndexer/TermDocIndexer.java 
jar -cvf bin/TermDocIndexer.jar -C TermDocIndexer/ .
