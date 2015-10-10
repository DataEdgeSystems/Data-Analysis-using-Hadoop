package yelp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private Text word = new Text();
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		String recordString = value.toString();
		String [] record = recordString.split("::");
		if(record.length==24 && record[22].equalsIgnoreCase("business")){
			
			word.set(record[2]);
			context.write(word, new Text("business::"+record[3]+"::"+record[10]));
			
		}
	}

}
