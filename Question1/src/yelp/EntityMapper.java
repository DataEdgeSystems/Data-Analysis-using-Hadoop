package yelp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*public class EntityMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private static Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		
		String recordString = value.toString();
		String[] str = recordString.split("::");
		for(int count=0;count<str.length;count++) {
			if(str[count].equalsIgnoreCase("review")) {
				word.set("review");
			} else if (str[count].equalsIgnoreCase("user")) {
				word.set("user");
			} else if (str[count].equalsIgnoreCase("business")) {
				word.set("business");
			} 
		}
		context.write(word,one);
	}*/
	
	public class EntityMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();  // type of output key


		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			String recordString = value.toString();
			String[] record = recordString.split("::");
			if(record.length==24) {
				if(record[22].equals("user")) {
					word.set("user");
				} else if(record[22].equals("review")) {
					word.set("review");
				} else if (record[22].equals("business")) {
					word.set("business");
				} else {
					word.set("Type");
				}
				context.write(word, one);
			} else {
				word.set("Random");
				context.write(word, one);
			}
		}
	}

