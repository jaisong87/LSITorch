javac -classpath $HADOOP_HOME/hadoop-core-0.20.203.0.jar  -d TermIndexer TermIndexer/TermIndexer.java
jar -cvf bin/TermIndexer.jar -C TermIndexer/ .
