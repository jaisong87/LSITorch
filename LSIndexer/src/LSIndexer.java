import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import Clustering.ClusteringMapper;
import Clustering.ClusteringReducer;
import CombineCentroids.CombineCentroidsMapper;
import CombineCentroids.CombineCentroidsReducer;
import MakeCluster.MakeClusterMapper;
import MakeCluster.MakeClusterReducer;



public class LSIndexer {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 6) {
      System.err.println("Usage: wordcount <in> <out> <NoOfClusters K> <CentroidFile> <MaxIterations> <ReducedRank>");
      System.exit(2);
    }
    
    conf.set("NoOfClusters", otherArgs[2]);
    int maxIter = Integer.parseInt(otherArgs[4]);
    int i=0;
    for(i=0;i<maxIter;i++) {
    	
      System.out.println(i + "th ITERATION");
    
      /* Cluster the inputs */
      conf.set("CentroidFile", otherArgs[3]+i+"/part-r-00000");    
      Job job = new Job(conf, "Clustering Job");
      job.setJarByClass(LSIndexer.class);
      job.setMapperClass(ClusteringMapper.class);
      job.setReducerClass(ClusteringReducer.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);
    
      job.setNumReduceTasks(Integer.parseInt(otherArgs[2]));
    
      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+i));
      job.waitForCompletion(true);
      
      /*Job to make the next centroid file*/
      System.out.println("Making the new Centroid FIle");
      Job job2 = new Job(conf, "Make Centroid");
      job2.setJarByClass(LSIndexer.class);
      job2.setMapperClass(CombineCentroidsMapper.class);
      job2.setReducerClass(CombineCentroidsReducer.class);
      job2.setOutputKeyClass(IntWritable.class);
      job2.setOutputValueClass(Text.class);
    
      job2.setNumReduceTasks(1);
    
      FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+i));
      int j=i+1;
      FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]+j));
      job2.waitForCompletion(true);
      
    }
    /* Job to make the final Cluster */
	System.out.println("Final MapReduce Job Running");
    
    conf.set("ReducedRank", otherArgs[5]);
    conf.set("CentroidFile", otherArgs[3]+i+"/part-r-00000");    
    Job job3 = new Job(conf, "Clustering Job");
    job3.setJarByClass(LSIndexer.class);
    job3.setMapperClass(MakeClusterMapper.class);
    job3.setReducerClass(MakeClusterReducer.class);
    job3.setOutputKeyClass(IntWritable.class);
    job3.setOutputValueClass(Text.class);
  
    job3.setNumReduceTasks(Integer.parseInt(otherArgs[2]));
  
    FileInputFormat.addInputPath(job3, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]+i));
    job3.waitForCompletion(true);

    /*END*/
  }
}
