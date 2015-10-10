package yelp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EntityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static Text word = new Text();
	private static IntWritable ratings = new IntWritable();
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		String recordString = value.toString();
		String [] record = recordString.split("::");
		if(record.length==24 && record[22].equalsIgnoreCase("review")) {
			word.set(record[2]);
			ratings.set(Integer.parseInt(record[20]));
			context.write(word,ratings);			
		}
		
	}

}
