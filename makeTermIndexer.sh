javac -classpath $HADOOP_HOME/*.jar  -d TermIndexer TermIndexer/TermIndexer.java
jar -cvf bin/TermIndexer.jar -C TermIndexer/ .
