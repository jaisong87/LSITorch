/*
 * TermIndexer.java - Identifies all the unique terms and assigns indexes to them
 * This is not required in the case of very large corpora where we can use a std 
 * indexing that has all the words in english dictionary
 * [ The script also calculates the total frequency of terms across all documents
 *   so that we can use it in computations like TF-IDF, GF-IDF ]
 */
	package org.myorg;
 	
 	import java.io.IOException;
 	import java.util.*;
 	
 	import org.apache.hadoop.fs.Path;
 	import org.apache.hadoop.conf.*;
 	import org.apache.hadoop.io.*;
 	import org.apache.hadoop.mapred.*;
 	import org.apache.hadoop.util.*;
 	
	/* Mapper<LineNumber, LineStr, Term, TermFrequency>  */	
 	public class TermIndexer {	
 	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
 	     private final static IntWritable one = new IntWritable(1);
 	     private Text word = new Text();
 		
	/* @arg key - Line number in the file
	 * @arg value - Line of text
         * @arg output -<K,V> pair for <Term, TermFrequency>
	 * @arg reporter - 
         */	
 	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
 	       String line = value.toString();
	       line = line.replaceAll("[^a-zA-Z0-9]+"," ");
	       line = line.toLowerCase();
 	       StringTokenizer tokenizer = new StringTokenizer(line);
 	       while (tokenizer.hasMoreTokens()) {
 	         word.set(tokenizer.nextToken());
 	         output.collect(word, one);
 	       }
 	     }
 	   }
 	
	/* @arg key - Term
         * @arg values - Term Frequencies
	 * @output - <Term, Term Frequency> collector from reduce
	 */	
 	   public static class reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
 	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
 	       int sum = 0;
 	       while (values.hasNext()) {
 	         sum += values.next().get();
 	       }
		/* Dont collect to reduce rather write straight-away to $OUTPUT_FOLDER/Index/TermIndex/ */
 	       output.collect(key, new IntWritable(sum));
 	     }
 	   }
 	
	/* Main hadoop Job for TermIndexing  */	
 	   public static void main(String[] args) throws Exception {
 	     JobConf conf = new JobConf(TermIndexer.class);
 	     conf.setJobName("TermIndexer");
 	
 	     conf.setOutputKeyClass(Text.class);
 	     conf.setOutputValueClass(IntWritable.class);
 	
 	     conf.setMapperClass(Map.class);
 	     conf.setCombinerClass(Reduce.class);
 	     conf.setReducerClass(Reduce.class);
 	
 	     conf.setInputFormat(TextInputFormat.class);
 	     conf.setOutputFormat(TextOutputFormat.class);
 	
 	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
 	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 	
 	     JobClient.runJob(conf);
 	   }
 	}
