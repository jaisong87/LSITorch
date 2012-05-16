javac -classpath $HADOOP_HOME/hadoop-0.20.2-core.jar  -d TermIndexer TermIndexer/TermIndexer.java
jar -cvf bin/TermIndexer.jar -C TermIndexer/ .
