javac -classpath "$HADOOP_HOME/$CORE_JAR"  -d TermIndexer TermIndexer/TermIndexer.java
jar -cvf bin/TermIndexer.jar -C TermIndexer/ .
