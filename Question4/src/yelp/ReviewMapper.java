package yelp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewMapper extends Mapper<LongWritable, Text, Text, Text>{

	private Text word = new Text();
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		String recordString = value.toString();
		String [] record = recordString.split("\\t");
		word.set(record[0]);
		context.write(word,new Text("review::"+record[1]));
		
		
	}
}
