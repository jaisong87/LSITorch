rm bin/TermDocIndexer.jar
javac -classpath "$HADOOP_HOME/$CORE_JAR" -d TermDocIndexer/ TermDocIndexer/TermDocIndexer.java 
jar -cvf bin/TermDocIndexer.jar -C TermDocIndexer/ .
