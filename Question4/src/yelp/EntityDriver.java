package yelp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class EntityDriver {

	public final static IntWritable num_reducers = new IntWritable(3);
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = new Job(conf,"ReduceSideJoin1");
		
		
		job.setJarByClass(EntityDriver.class);
		job.setMapperClass(EntityMapper.class);
		job.setReducerClass(EntityReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.waitForCompletion(true);
		
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"ReduceSideJoin2");
		
		//job1.setMapperClass(BusinessMapper.class);
		
		job1.setJarByClass(EntityDriver.class);
		
		MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class ,
				BusinessMapper.class );

		MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), TextInputFormat.class ,
				ReviewMapper.class);
		
		//job1.setNumReduceTasks(num_reducers.get());
		//job1.setPartitionerClass(CustomPartitioner.class);
		job1.setReducerClass(BusinessReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1,new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
	
		System.exit(job1.waitForCompletion(true) ? 0 : 1);		
	}
}
