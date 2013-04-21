package IDF_Computing;

/* compute IDF values
 * 执行：hadoop xx.jar HDFSFileName.input HDFSFileName.output IDFterms_file
 * 输入：hdfs: IDs as the samples of IDF computing
 * 输出：hdfs:  the occurrence number of each IDF term, 
 * WARNING!!! the 0 occureence term will not be output.
 * IDFterms_file  the file containing all the corpus terms
 */

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Driver {

    public static boolean checkAndDelete(final String path, Configuration conf) {
        Path dst_path = new Path(path);
        try {

                FileSystem hdfs = dst_path.getFileSystem(conf);

                if (hdfs.exists(dst_path)) {

                        hdfs.delete(dst_path, true);
                }
        } catch (IOException e) {
                e.printStackTrace();
                return false;
        }
        return true;

}
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 4) {
      System.err.println("Usage: toCreateTags <in> <out> <IDFterms_file> <arguments_file>");
      System.exit(2);
    }
    
    for(int i=2;i<otherArgs.length; i++)
      DistributedCache.addCacheFile((URI.create(otherArgs[i])), conf);
 
    Job job = new Job(conf, "To Compute IDF values");
    
    checkAndDelete(otherArgs[1],conf);
    job.setJarByClass(Driver.class);
    job.setMapperClass(idf_mapper.class);
    job.setReducerClass(idf_reducer.class);          //
    job.setMapOutputKeyClass(Text.class);  
    job.setMapOutputValueClass(IntWritable.class); 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
 //   HDFSTool.checkAndDelete(otherArgs[1], conf);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    job.setNumReduceTasks(5);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
      
  }
}
