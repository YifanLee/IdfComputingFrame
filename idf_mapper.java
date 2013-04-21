package IDF_Computing;

import java.io.BufferedReader;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.splitWord.Analysis;
import org.ansj.util.recognition.NatureRecognition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.util.Bytes;




public class idf_mapper extends Mapper<Object, Text, Text, IntWritable> {

  //private Text keyword = new Text();
	private static String start_time;
	private static String end_time;
	private static String content_table;
	private static String SW;
	private ArrayList<String> onemonth_content = new ArrayList<String>();
	private StringBuffer tags = new StringBuffer("");
	private ArrayList<String> paser = new ArrayList<String>();
	private ArrayList<String> notags = new ArrayList<String>();
    private static HTable table = null;
    private static HashSet<String> IDterms_set = new HashSet<String>();
    private HashSet<String> onemonthTerms_set = new HashSet<String>();
    private final static IntWritable one = new IntWritable(1);
    

		public ArrayList<String> scaneByStartEndKeys(String rowkey, HTable table, String ts, String te) { 
        	// for test:
        	//String time = t;
        	Scan scan = new Scan();
        	scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ct"));
        	scan.setStartRow(Bytes.toBytes(rowkey+"-"+ts));
        	scan.setStopRow(Bytes.toBytes(rowkey+"-"+te)); 
            paser = new ArrayList<String>();
            StringBuffer onemonth_tags = new StringBuffer("");
        	try{
        		ResultScanner rs = table.getScanner(scan);
        		try {
                	   
                	   for (Result r = rs.next(); r != null; r = rs.next())
                	   {
                		   onemonth_tags.append(Bytes.toString((r.getValue(Bytes.toBytes("info"), Bytes.toBytes("ct"))))).append(" ");
                		   //System.out.println(r.size()+"%%%%%%%%%%%%%%%%%%%%");
                		   //System.out.println("rowkey!!!!!!!");
                    	   //System.out.println(r.raw().toString());
                	   }
                	   //System.out.println("onemonth_tags@@@@@@@@@@@@");
                	   //System.out.println(onemonth_tags);
                	   //return onemonth_tags;
                	   if (!onemonth_tags.equals(""))
                	   {
                		   
                		   Analysis udf = new ToAnalysis(new StringReader(onemonth_tags.toString()));
                		   Term term = null;             		   
                		   while((term=udf.next())!=null)
                		   {
                			   //tagsbuf.append(" ").append(term.getName());
                			   paser.add(term.getName());
                			   //System.out.println(term.getName()+"--nature:"+term.getNatrue().toString());
                			   
                		   }
                		   //return tagsbuf.toString();   
                		   
                		   
                		   //ArrayList paser = ToAnalysis.paser(onemonth_tags);
                		   onemonth_tags = null;
                		   return paser;   
                		   
                	   }
                        
                   } 
                   catch (IOException e) {  
                         e.printStackTrace(); 
                   }
                rs.close();
                
        	}
        	catch(IOException e){
        		e.printStackTrace(); 
        		
        	}
         return notags; 
         }  

        
    	private void ClassifierSetup(Context context) throws IOException
    	{
    		
    		URI[] localFiles = DistributedCache.getCacheFiles(context.getConfiguration());
    		FileSystem fs = FileSystem.get(localFiles[0],context.getConfiguration());
    		FileSystem fs2 = FileSystem.get(localFiles[1],context.getConfiguration());
    		BufferedReader IDFtermsFile = new BufferedReader(new InputStreamReader(fs.open(new Path(localFiles[0]))));
    		BufferedReader argumentsfile = new BufferedReader(new InputStreamReader(fs2.open(new Path(localFiles[1]))));
    		//Predicates.init(linearModel, null, normalizer);
    		String line = null;
    		while((line = IDFtermsFile.readLine())!=null)
    		{
    			String[] linelist = line.split("\t");
    			if ((linelist[0]!=null) && (linelist[0]!=""))
    			{
    			//String[] conlist = linelist[1].split(",");
    			IDterms_set.add(linelist[0]);
    			//System.out.println("conlist ****************");
    			//System.out.println(conlist);
    			
    			}
    			
    		}
    		
    		start_time = argumentsfile.readLine();
    		end_time = argumentsfile.readLine();
    		content_table = argumentsfile.readLine();
    		if (content_table.equals(new String("Sina_Weibo_Content")))
    		{
    			SW = "sw";
    		}
    		/*
    		System.out.println("arguments ****************");
			System.out.println(start_time);
			System.out.println(end_time);
			System.out.println(content_table);
			System.out.println(SW);
			*/
    	    if (table == null)
    	    	{
    	    	Configuration conf=null;
    	        conf = HBaseConfiguration.create();  
    	        conf.set("hbase.zookeeper.quorum", "master,slave01,slave02");
    	        conf.set("hbase.zookeeper.property.clientPort", "2181");
    	        try {
    	                        //table=new HTable(conf,"Sina_Weibo_Content");
    	        				table=new HTable(conf,content_table);
    	                } catch (IOException e) {
    	                        // TODO Auto-generated catch block
    	                        e.printStackTrace();
    	                }
    	    	}

    		
    		
    		argumentsfile.close();
    		//conceptlists.close();
    		//normalizer.close();
    		
    	}
    	
    	
    	@Override
    	//IDF dictionary load and schema assemble
    	protected void setup(Context context) throws IOException
    	{
    		//SearchSetup(context);
    		ClassifierSetup(context);
    		
    	}
        
        public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {

//                      System.out.println(value.toString());

                        paser = null;
                        onemonth_content = new ArrayList<String>(scaneByStartEndKeys(value.toString(),table,start_time,end_time));
                        //System.out.println("onemonth_content!!!!!!!!!!!!!!!");
                        //System.out.println(onemonth_content);
                        if(onemonth_content.isEmpty())
                            return;
                        for (int i = 0; i < onemonth_content.size();i++)
                        {
                        	onemonthTerms_set.add(onemonth_content.get(i));
                        	
                        }

                        onemonth_content = null;
                        
                        Text keyword = new Text();
                        String term = new String();
                        Iterator<String> iterator1=onemonthTerms_set.iterator();
                		while(iterator1.hasNext()){
                			//System.out.println(iterator1.next());
                			term = iterator1.next();
                			if(IDterms_set.contains(term))
                			{
                				keyword.set(term);
                				context.write(keyword,one);
                			}
                				
                		}
                        
                        

                        //Text keyword = new Text(SW+"-"+value.toString());
                        //System.out.println(onemonth_content+"%%%%%%%%%%%%%%%%%%%%");
                        //context.write(keyword,new Text(tags.toString()));
                        tags = null;
                        onemonthTerms_set.clear();
                        //System.out.println(onemonth_content+"%%%%%%%%%%%%%%%%%%%%");
                        //keyword.clear();
                        
        }
}


