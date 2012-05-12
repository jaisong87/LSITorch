echo "USing hadoop from $HADOOP_HOME \n"
echo "Making TermIndex from $HADOOP_INP into $HADOOP_OUT \n"
$HADOOP_HOME/bin/hadoop jar ../bin/TermIndexer.jar org.myorg.TermIndexer $HADOOP_INP $HADOOP_OUT
