#Script to run for preprocesing the input files and get indices for terms
echo "USing hadoop from $HADOOP_HOME \n"
HADOOP_OUT="$HADOOP_INP/../Index/"
echo "Making TermIndex from $HADOOP_INP into $HADOOP_OUT \n"
$HADOOP_HOME/bin/hadoop jar ../bin/TermIndexer.jar org.myorg.TermIndexer $HADOOP_INP
$HADOOP_HOME/bin/hadoop fs -cat "$HADOOP_OUT/part*" > tmpIndex
$HADOOP_HOME/bin/hadoop fs -rm "$HADOOP_OUT/*" 
$HADOOP_HOME/bin/hadoop fs -rmr "$HADOOP_OUT/*" 
sort -r -k 2 -n tmpIndex > tmpIndex2
$HADOOP_HOME/bin/hadoop fs -put tmpIndex2 "$HADOOP_OUT/TermIndex.dat"
rm tmpIndex
rm tmpIndex2
$HADOOP_HOME/bin/hadoop fs -ls $HADOOP_INP > tmpIndex2
awk < tmpIndex2 '{print $8}' > DocIndex.dat
rm tmpIndex2
$HADOOP_HOME/bin/hadoop fs -put DocIndex.dat  "$HADOOP_OUT/DocIndex.dat"
rm DocIndex.dat 
