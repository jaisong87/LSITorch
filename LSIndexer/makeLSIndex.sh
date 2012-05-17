$HADOOP_HOME/bin/hadoop fs -cat "$HADOOP_INP/../TDMatrix/part*" > tmp
head -n $CLUSTERS tmp > tmp2
CENTROID_DIR="$HADOOP_INP/../Centroid"
LSI_INDEX="$HADOOP_INP/../LSIndex"
echo "Initializing centroids"
$HADOOP_HOME/bin/hadoop fs -copyFromLocal tmp2 "$HADOOP_INP/../Centroid0/part-r-00000"
rm tmp
rm tmp2
echo "Generating LSIndex from $HADOOP_INP/../TDMatrix/ to $LSI_INDEX"
$HADOOP_HOME/bin/hadoop jar ../bin/LSIndexer.jar LSIndexer "$HADOOP_INP/../TDMatrix/" $LSI_INDEX $CLUSTERS $CENTROID_DIR $MAX_ITER $REDUCED_RANK 
