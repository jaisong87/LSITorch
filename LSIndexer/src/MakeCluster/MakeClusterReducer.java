package MakeCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import Jama.Matrix;
import Jama.SingularValueDecomposition;


  public class MakeClusterReducer 
       extends Reducer<IntWritable, Text, Text, IntWritable> {
    
	       public void reduce(IntWritable key, Iterable<Text> values, 
			       Context context
			       ) throws IOException, InterruptedException {

		       ArrayList<String> list = new ArrayList<String>();

		       for (Text val : values) {
			       list.add(val.toString()); 
			       context.write(val, null);
		       }
		       int coloumnSize = list.size();
		       int rowSize = list.get(0).split("\t").length-1;       
		       if (coloumnSize == 0) {
			       System.out.println("Cannot perform LSI because '0' elements in this Cluster");
		       }
	
		/* Need to look into this condition*/
		       else if (rowSize <= coloumnSize) { 
			       System.out.println("rowSize =" + rowSize);
			       System.out.println("coloumnSize =" + coloumnSize);
			       System.out.println("Cannot perform LSI because rowsize < coloumnsize in this Cluster");
		       }
			else {
       double [][] arr = new double[rowSize][coloumnSize];
       String[] Ids = new String[coloumnSize];
       
       Iterator<String> ListIter = list.iterator();
       
       int countRow = 0;
       int countCol = 0;
       
       while (ListIter.hasNext()) {
    	   String tmp = ListIter.next();
    	   StringTokenizer itr = new StringTokenizer(tmp);
    	   Ids[countCol] = itr.nextToken("\t");
    	   
           while (itr.hasMoreTokens()) {
        	   arr[countRow][countCol] = Double.parseDouble(itr.nextToken("\t"));
               //context.write(word, one);
               countRow++;
             } countRow=0;
             countCol++;  
       }

       Matrix A = new Matrix(arr);
       SingularValueDecomposition SVD = new SingularValueDecomposition(A);

       Matrix S = SVD.getS();
       Matrix U = SVD.getU();
       Matrix V = SVD.getV();

       Configuration conf = context.getConfiguration();
       int reducedRank = Integer.parseInt(conf.get("ReducedRank"));
       int rank= S.rank();
       int toreduce = 40; //default no specific reason why I gave 40
       if((6*rank)/10 < reducedRank ) {
	       toreduce = (6*rank)/10;
       }
       else {
	       toreduce = reducedRank;
       }

       //Reducing rank
       S = S.getMatrix(0, toreduce-1, 0, toreduce-1);
       U = U.getMatrix(0, U.getRowDimension()-1, 0, toreduce-1);
       V = V.getMatrix(0, V.getRowDimension()-1, 0, toreduce-1);

       A = U.times(S);
       A = A.times(V.transpose());

       //Convert back to array to print
       arr = A.getArray();
       String val = "";
       for (int i=0;i<coloumnSize;i++) {
	       val = Ids[i]+" ";
	       for (int j=0;j<rowSize;j++) {
		       val += Double.toString(arr[j][i]) + " ";
	       }
	       context.write(new Text(val), null);
       }

       }
       
    }
  }
