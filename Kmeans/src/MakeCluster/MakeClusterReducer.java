package MakeCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

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
			       return;
		       }
	
		/* Need to look into this condition*/
		       if (rowSize <= coloumnSize) { 
			       System.out.println("rowSize =" + rowSize);
			       System.out.println("coloumnSize =" + coloumnSize);
			       System.out.println("Cannot perform LSI because rowsize < coloumnsize in this Cluster");
			       return;
		       }

       double [][] arr = new double[rowSize][coloumnSize];
       String[] Ids = new String[coloumnSize];
       
       Iterator<String> ListIter = list.iterator();
       
       int countRow = 0;
       int countCol = 0;
       
       while (ListIter.hasNext()) {
    	   String tmp = ListIter.next();
    	   StringTokenizer itr = new StringTokenizer(tmp);
    	   Ids[countCol] = itr.nextToken();
    	   
           while (itr.hasMoreTokens()) {
        	   arr[countRow][countCol] = Double.parseDouble(itr.nextToken());
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
        //decrease the rank of S here for Latent Semantic Indexing
        A = U.times(S);
        A = A.times(V.transpose());
        
       
       /*for (int i=0;i<rowSize;i++) {
    	   for (int j=0;j<coloumnSize;j++){
               System.out.print(arr[i][j] + " ");
    	   }   System.out.println();
       }
       
       for (int j=0;j<coloumnSize;j++){
            System.out.print(Ids[j] + " ");
       } */  
       
       
    }
  }
