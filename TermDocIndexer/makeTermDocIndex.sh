#Script to run for preprocesing the input files and get indices for terms
echo "USing hadoop from $HADOOP_HOME \n"
echo "Making TermIndex from $HADOOP_INP into $HADOOP_OUT \n"
$HADOOP_HOME/bin/hadoop jar ../bin/TermIndexer.jar org.myorg.TermIndexer $HADOOP_INP $HADOOP_OUT
$HADOOP_HOME/bin/hadoop fs -cat /user/aelikkottil/Dataset-2/Index/part* > tmpIndex
$HADOOP_HOME/bin/hadoop fs -rm /user/aelikkottil/Dataset-2/Index/* 
$HADOOP_HOME/bin/hadoop fs -rmr /user/aelikkottil/Dataset-2/Index/* 
$HADOOP_HOME/bin/hadoop fs -put tmpIndex /user/aelikkottil/Dataset-2/Index/TermIndex.dat
rm tmpIndex
$HADOOP_HOME/bin/hadoop fs -ls $HADOOP_INP > tmpIndex2
awk < tmpIndex2 '{print $8}' > DocIndex.dat
rm tmpIndex2
$HADOOP_HOME/bin/hadoop fs -put DocIndex.dat  $HADOOP_OUT/DocIndex.dat
rm DocIndex.dat 
