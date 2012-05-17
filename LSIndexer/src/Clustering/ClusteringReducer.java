package Clustering;
import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


  public class ClusteringReducer 
       extends Reducer<IntWritable, Text, Text, IntWritable> {
    
	       public void reduce(IntWritable key, Iterable<Text> values, 
			       Context context
			       ) throws IOException, InterruptedException {

		       double[] intArray = null;
		       int flag =1;
		       int size = 0;
		       int counter = 0;
		       //key is unique for each reducer hence append C to it
		       //the value key ranges from 0 to K-1


		       for (Text val : values) {
			       System.out.println(val.toString());
			       if (flag == 1) {
				       size = val.toString().split("\t").length - 1;
				       intArray = new double[size];
				       for (int i=0; i < size ; i++)
					       intArray[i] = 0;    		  
				       flag =0;
			       }

			       String[] arr = val.toString().split("\t");
			       for (int i=0; i<size;i++)
				       intArray[i] += Double.parseDouble(arr[i+1]);
			       counter++;
		       }

		       for (int i=0;i<size;i++)
			       intArray[i] = intArray[i]/counter;

		       String toWrite ="C" + key + "\t";
		       for (int i=0;i<size;i++)
			       toWrite = toWrite + intArray[i] + "\t";

		       Text toWriteText =  new Text(toWrite);
		       context.write(toWriteText, null); 
	       }
       }
