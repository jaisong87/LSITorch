#Specify your hadoop installation directory here
export HADOOP_HOME=/home/hadoop_newton/hadoop_install/
#export HADOOP_HOME=/home/jaison/hadoop-0.20.2/

#specify the name of hadoop jar ( could vary with different versions of hadoop)
export CORE_JAR=hadoop-core-0.20.203.0.jar
#export CORE_JAR=hadoop-0.20.2-core.jar

#Specify your input folder here where the corpus is present
export  HADOOP_INP=/user/aelikkottil/ACL-Small/Corpus/
#export HADOOP_INP=/Dataset-2/Corpus/
#export HADOOP_INP=/user/aelikkottil/ACL-Dataset/Corpus/

#Specifty the number of clusters required 
export CLUSTERS=100

#Specify the maximum number of iterations
export MAX_ITER=10

#Specify the reduced rank for the matrix ( 300-500 is typical)
export REDUCED_RANK=30

#Specify the nature of input file (Plain-Text means a set of simple text file, Dump means a single file containing all the documents(splitter required) - )
#Dump is much faster and recommended than 1000's of plain files. currently dump option is not integrated into this folder ( source is attached in the code used on wikipedia data)
export INP_TYPE=Plain-Text

#Specify the maximum number of terms to use here
export TERM_COUNT=200

#Matrix type INC - incidence matrix, TF -term frequency, TFIDF - term frequency inverse document frequency
export MAT_TYPE=TFIDF
