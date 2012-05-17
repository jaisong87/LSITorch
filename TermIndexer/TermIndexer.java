/*
 * TermIndexer.java - Identifies all the unique terms and assigns indexes to them
 * This is not required in the case of very large corpora where we can use a std 
 * indexing that has all the words in english dictionary
 * [ The script also calculates the total frequency of terms across all documents
 *   so that we can use it in computations like TF-IDF, GF-IDF ]
 */
	package org.myorg;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	
	/* Mapper<LineNumber, LineStr, Term, TermFrequency>  */	
 	public class TermIndexer {	
 	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> { 
 	     private final static IntWritable one = new IntWritable(1);
 	     private Text word = new Text();
 	     String inpFile = "";

	     public void setup(Context context) {
		     System.out.println("============== SETUP ()  ============");
		     FileSplit fileSplit = (FileSplit)context.getInputSplit();
		     inpFile = fileSplit.getPath().getName();
	     }

	/* @arg key - Line number in the file
	 * @arg value - Line of text
         * @arg output -<K,V> pair for <Term, TermFrequency>
	 * @arg reporter - 
         */	
		public void map(LongWritable key, Text value, Context context) throws IOException {
 	       String line = value.toString();
	       line = line.replaceAll("[^a-zA-Z0-9]+"," ");
	       line = line.toLowerCase();
 	       StringTokenizer tokenizer = new StringTokenizer(line);
 	       while (tokenizer.hasMoreTokens()) {
 	         word.set(tokenizer.nextToken());

		 try {
			 context.write(word, new Text(inpFile));
		 } catch (InterruptedException e) {
			 // TODO Auto-generated catch block
			 System.out.println("Failed emitting from mapper");
			 e.printStackTrace();
		 }

 	       }
 	     }
 	   }
 	
	/* @arg key - Term
         * @arg values - Term Frequencies
	 * @output - <Term, Term Frequency> collector from reduce
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values,
                                Context context)
                                throws IOException {
		Set<String> fileSet = new HashSet<String>();
 	       int sum = 0, dc = 0;
 	       for(Text val:values) {
 	         fileSet.add(val.toString());
		 sum++;
 	       }
		dc = fileSet.size();
		/* Dont collect to reduce rather write straight-away to $OUTPUT_FOLDER/Index/TermIndex/ */
		Text res = new Text(sum+"\t"+dc);

		try {
			context.write(key, res);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println("Failed emitting from mapper");
			e.printStackTrace();
		}

 	     }
 	   }
 	
	/* Main hadoop Job for TermIndexing  */	
 	   public static void main(String[] args) throws Exception {
		   Configuration conf = new Configuration();
		   Job job = new Job(conf, "TermIndexer Job");

                   job.setOutputKeyClass(Text.class);
                   job.setOutputValueClass(Text.class);

                   job.setJarByClass(TermIndexer.class);
                   job.setMapperClass(Map.class);
                   job.setReducerClass(Reduce.class);

                   FileInputFormat.setInputPaths(job, new Path(args[0]));
                   FileOutputFormat.setOutputPath(job, new Path(args[0]+"/../Index/"));

                   job.waitForCompletion(true);

 /*
	     JobConf conf = new JobConf(TermIndexer.class);
 	     conf.setJobName("TermIndexer");
 
 	     conf.setMapOutputKeyClass(Text.class);
 	     conf.setMapOutputValueClass(Text.class);
	
 	     conf.setOutputKeyClass(Text.class);
 	     conf.setOutputValueClass(Text.class);
		
 	     conf.setMapperClass(Map.class);
 	     //conf.setCombinerClass(Reduce.class);
 	     conf.setReducerClass(Reduce.class);
 	
 	     conf.setInputFormat(TextInputFormat.class);
 	     conf.setOutputFormat(TextOutputFormat.class);
 	
 	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
 	     FileOutputFormat.setOutputPath(conf, new Path(args[0]+"/../Index/"));
 	
 	     JobClient.runJob(conf);
 */
	   }
 	}
