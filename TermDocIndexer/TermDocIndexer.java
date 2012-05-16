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
import java.io.*;
/*
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.FileSystem;
*/
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

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
 	public class TermDocIndexer {
/*
		public class IntArrayWritable extends ArrayWritable { 
			public IntArrayWritable() { super(IntWritable.class); } 
			public IntArrayWritable(IntWritable[] values) {
				super(IntWritable.class, values);
			}
		}
*/
 	   public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
 	     private Text word = new Text();
 	     private HashMap<String, Integer > docIndex  = new HashMap<String, Integer>();
 	     private HashMap<String, Integer > termIndex = new HashMap<String, Integer>();
	     private HashMap<String, Integer > termFrq   = new HashMap<String, Integer>(); /* Used for TF-IDF, GF-IDF */
	     private int docCount = 0, termCount =0;	
	     private IntWritable curDocId = new IntWritable(-100); 	

		public Map()
			{
				System.out.println("============CALLING THE CONSTRUCTOR==============");
			}	
	     /* Construct the docIndex and termIndex before
	      * continuing with the termDocIndex
	      */		
	     public void setup(Context context) {
		     Configuration conf = context.getConfiguration(); 	
		     docCount = termCount = 0;
		int maxTermCount = Integer.parseInt(conf.get("TermCount"));
		curDocId.set(-1);			
		/* Fetch information about terms from termIndex */
		System.out.println("Trying to make termIndex");
		     try {
			     FileSystem fs = FileSystem.get(conf);
			     String termIdx = conf.get("TermIndex");	
			     Path infile = new Path(termIdx); /* TermIndex File*/
			     if (!fs.exists(infile))
				     System.out.println(termIdx+" does not exist");
			     if (!fs.isFile(infile))
				     System.out.println(termIdx+" is not a file");
		System.out.println("----Making termIndex-----");

			     BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(infile)));
			     String curLine = "", curTerm = "";
			     int curFrq = 0;			     
 				while( (curLine = br.readLine()) != null)
				{
					String tokens[] = curLine.split("\t");
				//System.out.println(curLine+" "+tokens.length);
					if(tokens.length>=2)
						{
						curTerm = tokens[0];
						curFrq = Integer.parseInt(tokens[1]);
						termFrq.put(curTerm, curFrq);
						termIndex.put(curTerm , termCount);
						System.out.println(termCount+" "+curTerm+" "+curFrq);
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
			   System.out.println("----Making DocIndex-----");
			     FileSystem fs = FileSystem.get(conf);
			     String docIdx = conf.get("DocIndex");
			     Path infile = new Path(docIdx); /* DocIndex File*/
			     if (!fs.exists(infile))
				     System.out.println(docIdx+" does not exist");
			     if (!fs.isFile(infile))
				     System.out.println(docIdx+" is not a file");

			     BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(infile)));
			     String curLine = "", curDoc = "";
 				while( (curLine = br.readLine()) != null)
				{
						curDoc = curLine.trim();
					if(curDoc.length()>0)
						{
						String tokens[] = curDoc.split("/");
						String curFile = tokens[tokens.length-1];			
						docIndex.put(curFile, docCount);
						System.out.println(docCount+" "+curFile);
						docCount++;
						}
				}
			     br.close();

		     } catch (IOException e) {
			     // TODO Auto-generated catch block
			     System.out.println("Exception in Creating the FileSystem object in Mapper");
			     e.printStackTrace();
		     }
		
		System.out.println("Initiaing TermDocIndexer with "+docCount+" documents using "+termCount+" terms");
	
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
      		String inputFile = fileSplit.getPath().getName();
		System.out.println("===========INPUT-FILE : "+inputFile+" =================");		

		if(docIndex.containsKey(inputFile)==true)
			{
 	 		curDocId.set(docIndex.get(inputFile));
			}
		System.out.print("Imp!!! - Running on inputFile "+inputFile+" with docIndex "+curDocId.get());
		//conf.set("TermCount", Integer.toString(termCount) );
		conf.set("DocCount", Integer.toString(docCount) );
		return;
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
		
		IntWritable dummyDocId = new IntWritable();
		dummyDocId.set(1);		
		
		Configuration conf = context.getConfiguration(); 	
		System.out.println("Working on TermDocIndexer with "+docCount+" documents using "+termCount+" terms");
		System.out.println("Conf : Working on TermDocIndexer with "+conf.get("DocCount")+" documents using "+conf.get("TermCount")+" terms");
		
		System.out.println("TermIndex has "+termIndex.size()+" elements and docIndex has "+docIndex.size()+" elements");		
		System.out.println("Line is : "+line);
 	       StringTokenizer tokenizer = new StringTokenizer(line);
 	       
	       while (tokenizer.hasMoreTokens()) {
		       String curTerm = tokenizer.nextToken();
			//System.out.println("curTerm is "+curTerm);
			//System.out.println("DocId is :"+curDocId.get());
		       
		       int termIdx = -1;

		       if(termIndex.containsKey(curTerm) == true)
		       		termIdx = termIndex.get(curTerm);

		       String ans = termIdx+"\t1";
			System.out.println("Emit :"+ curDocId.get()+" => "+ans);
		       Text ans1 = new Text(ans);

		       try {
				context.write( curDocId , ans1 );
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
 	   public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {

 		   public void reduce(IntWritable docId, Iterable<Text> termFrequencies,
				Context context)
				throws IOException {
			/* Dont collect to reduce rather write straight-away to $OUTPUT_FOLDER/Index/TermIndex/ */
			int[] finalTermFrq = new int[2];
		
		Configuration conf = context.getConfiguration(); 	
		int termCount = 1000;

		/*
  	 	conf.setIfUnset("TermCount", "1000");		
		String termCountString = conf.get("TermCount");
				
		if(termCountString != null)
			termCount = Integer.parseInt(termCountString);
		*/

		System.out.println("Reducer Conf : Working on TermDocIndexer with "+conf.get("DocCount")+" documents using "+conf.get("TermCount")+" terms");
			int[] docVector = new int[termCount];			

			int termFreq = 0;

	//		while(termFrequencies.hasNext())
		for (Text termInformation : termFrequencies)
			{
				String termInfo[] = termInformation.toString().split("\t");
				System.out.println("Input is : "+termInformation);
				if(termInfo.length>=2)
				{
					int curTermIdx = Integer.parseInt(termInfo[0]);
					int curTermFrq = Integer.parseInt(termInfo[1]);
					if(curTermIdx<termCount && curTermIdx>=0)
						docVector[curTermIdx] += curTermFrq;
					else {
						System.out.println("ERROR !! - TermIdx "+curTermIdx+" with frequency "+curTermFrq+" out of range");
					}
				}
			}
			
			//val.set(finalTermFrq);
			String val = "";
			for(int i=0;i<termCount;i++)
				val+=docVector[i]+"\t";
			try {
				context.write(docId, new Text(val));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("Failed emitting from reducer");
				e.printStackTrace();
			}
		}
 	   }
 	
	
	/* Main hadoop Job for TermIndexing  */	
 	   public static void main(String[] args) throws Exception {
 		   Configuration conf = new Configuration();
 		   conf.set("TermIndex",args[0]+"/../Index/TermIndex.dat");	
  	       conf.set("DocIndex",args[0]+"/../Index/DocIndex.dat");	
	       conf.set("TermCount", "1000");  		
 
 		   //JobConf conf = new JobConf(TermDocIndexer.class);
  	       Job job = new Job(conf, "TermDocIndexing Job");
  	    
 	     job.setOutputKeyClass(IntWritable.class);
 	     job.setOutputValueClass(Text.class);
 	
	     job.setJarByClass(TermDocIndexer.class);	
 	     job.setMapperClass(Map.class);
 	     //conf.setCombinerClass(Reduce.class);
 	     job.setReducerClass(Reduce.class);
 	
 	    // job.setInputFormat(TextInputFormat.class);
 	    // conf.setOutputFormat(TextOutputFormat.class);
 	
 	    FileInputFormat.setInputPaths(job, new Path(args[0]));
 	    FileOutputFormat.setOutputPath(job, new Path(args[0]+"/../TDMatrix/"));
 	
 	    job.waitForCompletion(true);
 	   }

 	}
