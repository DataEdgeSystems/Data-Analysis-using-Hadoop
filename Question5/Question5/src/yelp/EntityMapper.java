package yelp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EntityMapper extends Mapper<LongWritable, Text, Text, Text>{

	private Text userid = new Text();
	private Text review = new Text();
	private String businessid;
	private ArrayList<String> businessIdList = new ArrayList<String>();

	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		String recordString = value.toString();
		String [] record = recordString.split("::");
		if(record.length==24 && record[22].equalsIgnoreCase("review"))  {
			
			if(businessIdList.contains(record[2])) {
				
				userid.set(record[8]);
				review.set(record[1]);
				context.write(userid, review);
			}
		}
		
	}
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {

		super.setup(context);
		Configuration conf = context.getConfiguration();
		businessid = conf.get("businessid");
		Path[] localPaths = context.getLocalCacheFiles();
				   
		for(Path myfile:localPaths)
	    {
	        String line=null;
	        String nameofFile=myfile.getName();
	        File file =new File(nameofFile+"");
	        FileReader fr= new FileReader(file);
	        BufferedReader br= new BufferedReader(fr);
	        line=br.readLine();
	        while(line!=null)
	        {
	            String[] arr=line.split("::");
	            if(arr[22].equalsIgnoreCase("business") && arr[3].contains("Stanford"))
	            	businessIdList.add(arr[2]); 
	            
	            line=br.readLine();
	        
	        }
	    }
	
	}

}
