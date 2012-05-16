package MakeCluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MakeClusterMapper 
       extends Mapper<Object, Text, IntWritable, Text>{
    
    private HashMap<Integer, String> Centroids = new HashMap<Integer, String>();

    public void setup(Context context) {
	    Configuration conf = context.getConfiguration();
	    try {
		    FileSystem fs = FileSystem.get(conf);
		    Path infile = new Path(conf.get("CentroidFile")); //give it as an argument
		    if (!fs.exists(infile))
			    System.out.println("Centroids.txt does not exist");
		    if (!fs.isFile(infile))
			    System.out.println("Centroids.txt is not a file");
		    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(infile)));
		    String str = conf.get("NoOfClusters");

		    int K = Integer.parseInt(str);
		    for (int i=0; i<K;i++)
				{
			    String centroidData = br.readLine();
			    Centroids.put(i, centroidData);
			System.out.println(centroidData);
				}

		    br.close();

	    } catch (IOException e) {
		    // TODO Auto-generated catch block
		    System.out.println("Exception in Creating the FileSystem object in Mapper");
		    e.printStackTrace();
	    }   

    }
  
    /* Calculate the euclidian distance between two vectors*/  
    private double EucledianDistance(String s1, String s2) {
	    StringTokenizer itr1 = new StringTokenizer(s1);
	    StringTokenizer itr2 = new StringTokenizer(s2);
	    String id1 = itr1.nextToken("\t");//Do not remove (needed to remove the first id element)
	    String id2 = itr2.nextToken("\t");//Do not remove (needed to remove the first id element)
	    Double sum = 0.0;
	    Double Diff = 0.0;
	    while (itr1.hasMoreTokens() && itr2.hasMoreTokens()) {
		    Diff = (Double.parseDouble(itr1.nextToken("\t")) - Double.parseDouble(itr2.nextToken("\t")));
		    sum = sum + Math.pow(Diff, 2);          
	    }

	    return Math.sqrt(sum);
    }

    
    public void map(Object key, Text value, Context context
		   ) throws IOException, InterruptedException {

	    int minCentroidID=0;
	    double dist, mindist = -1.0;
	    Iterator<Integer> keyIter = Centroids.keySet().iterator();
	    while (keyIter.hasNext()) {

		    Integer CentroidID = keyIter.next();
		    dist = EucledianDistance(Centroids.get(CentroidID), value.toString());
		    System.out.println("Distance between"+Centroids.get(CentroidID)+"AND"+value.toString()+"is"+ dist);
		    if (dist < mindist || mindist == -1.0) {
			    mindist = dist;
			    minCentroidID = CentroidID;
		    }
	    }

	      IntWritable k = new IntWritable(minCentroidID);
	      context.write(k, value);

    }
  }
