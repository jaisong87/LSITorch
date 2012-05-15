/*
 * TermDocIndexer.java - Identifies all the unique terms and assigns indexes to them
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.FileSystem;

	/* Mapper<LineNumber, LineStr, Term, TermFrequency>  */	
 	public class TermDocIndexer {	
 	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
 	     private final static IntWritable one = new IntWritable(1);
 	     private Text word = new Text();
 	     private HashMap<String, Integer > docIndex;
 	     private HashMap<String, Integer > termIndex;
	     private HashMap<String, Integer > termFrq;
	     private int docCount, termCount;	
	     private int curDocId; 	
	     IntWritable[] docVector;// = new IntWritable[termIndex.length];
	
	     /* Construct the docIndex and termIndex before
	      * continuing with the termDocIndex
	      */		
	     public void setup(Context context) {
		     Configuration conf = context.getConfiguration(); 	
		     docCount = termCount = 0;
			
		/* Fetch information about terms from termIndex */
		     try {
			     FileSystem fs = FileSystem.get(conf);
			     String termIdx = conf.get("TermIndex");
			     Path infile = new Path(termIdx); /* TermIndex File*/
			     if (!fs.exists(infile))
				     System.out.println(termIdx+" does not exist");
			     if (!fs.isFile(infile))
				     System.out.println(termIdx+" is not a file");

			     BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(infile)));
			     string curLine = "", curTerm = "";
			     int curFrq = 0;			     
 				while(curLine = br.readLine())
				{
					String tokens[] = curLine.split(" ");
					if(tokens.length>=2)
						{
						curTerm = tokens[0];
						curFrq = Integer.parseInt(tokens[1]);
						termFrq.put(curTerm, curFrq);
						termIndex.put(curTerm , termCount);
						termCount++;
						}
				}
			     br.close();

		     } catch (IOException e) {
			     // TODO Auto-generated catch block
			     System.out.println("Exception in Creating the FileSystem object in Mapper");
			     e.printStackTrace();
		     }

		/* Fetch information about documents from docIndex */
		     try {
			     FileSystem fs = FileSystem.get(conf);
			     String docIdx = conf.get("DocIndex");
			     Path infile = new Path(docIdx); /* DocIndex File*/
			     if (!fs.exists(infile))
				     System.out.println(docIdx+" does not exist");
			     if (!fs.isFile(infile))
				     System.out.println(docIdx+" is not a file");

			     BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(infile)));
			     string curLine = "", curDoc = "";
 				while(curLine = br.readLine())
				{
						curDoc = curLine.trim();
						docIndex.put(curDoc, docCount);
						docCount++;
				}
			     br.close();

		     } catch (IOException e) {
			     // TODO Auto-generated catch block
			     System.out.println("Exception in Creating the FileSystem object in Mapper");
			     e.printStackTrace();
		     }
		
		System.out.println("Initiaing TermDocIndexer with "+docCount+" documents using "+termCount+" terms");
         	String mapTaskId = conf.get("mapred.task.id");
         	String inputFile = conf.get("mapred.input.file");
	 	curDocId = docIndex[inputFile];
		IntWritable[] docVector = new IntWritable[termIndex.length];
		for(int i=0;i<termIndex.size();i++)
			docVector[i] = 0;
		return;
	     }

	/* @arg key - Line number in the file
	 * @arg value - Line of text
         * @arg output -<K,V> pair for <Term, TermFrequency>
	 * @arg reporter - 
         */	
 	     public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
 	       String line = value.toString();
	       line = line.replaceAll("[^a-zA-Z0-9]+"," ");
	       line = line.toLowerCase();
 	       StringTokenizer tokenizer = new StringTokenizer(line);
 	       while (tokenizer.hasMoreTokens()) {
 	         output.collect(curDocId, termIndex.get(tokenizer.nextToken()));
 	       }
 	     }
 	   }
 	
	/* @arg key - Term
         * @arg values - Term Frequencies
	 * @output - <Term, Term Frequency> collector from reduce
	 */	
 	   public static class Reduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable[]> {
 	     public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable[] > output, Reporter reporter) throws IOException {
		/* Dont collect to reduce rather write straight-away to $OUTPUT_FOLDER/Index/TermIndex/ */
 	       //output.collect(key, docVector);
 	     }
 	   }
 	
	   public void configure(JobConf job) {
         String mapTaskId = job.get("mapred.task.id");
         String inputFile = job.get("mapred.input.file");
	 curDocId = docIndex.get(inputFile);
       }	
	
	/* Main hadoop Job for TermIndexing  */	
 	   public static void main(String[] args) throws Exception {
 	     JobConf conf = new JobConf(TermDocIndexer.class);
 	     conf.setJobName("TermDocIndexer");
 	     conf.set("TermIndex","/user/aelikkottil/Dataset-2/Index/TermIndex.dat");	
 	     conf.set("DocIndex","/user/aelikkottil/Dataset-2/Index/DocIndex.dat");	

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