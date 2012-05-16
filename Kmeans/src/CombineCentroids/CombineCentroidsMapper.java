package CombineCentroids;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CombineCentroidsMapper 
       extends Mapper<Object, Text, IntWritable, Text>{
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
    	  IntWritable one = new IntWritable(1);
	      context.write(one, value);

    }
  }