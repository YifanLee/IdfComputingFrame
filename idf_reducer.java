package IDF_Computing;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
 


public class idf_reducer 
extends Reducer<Text,IntWritable,Text,IntWritable> {

  private IntWritable result = new IntWritable();  
	  
    public void reduce(Text key, Iterable<IntWritable> values,  
                       Context context  
                       ) throws IOException, InterruptedException {  
      int sum = 0;  
      for (IntWritable val : values) 
      {  
        sum += val.get();  
      }  
      result.set(sum);  
      context.write(key, result);  
    } 
             	
        	
 }

