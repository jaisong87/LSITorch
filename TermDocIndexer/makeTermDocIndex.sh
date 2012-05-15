#Script to run for preprocesing the input files and get indices for terms
echo "USing hadoop from $HADOOP_HOME \n"
$HADOOP_OUT="$HADOOP_INP/../TDMatrix"
echo "Making TermDocIndex from $HADOOP_INP into $HADOOP_OUT \n"
$HADOOP_HOME/bin/hadoop jar ../bin/TermDocIndexer.jar org.myorg.TermDocIndexer $HADOOP_INP
