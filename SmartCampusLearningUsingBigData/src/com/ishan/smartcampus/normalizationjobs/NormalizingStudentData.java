package com.ishan.smartcampus.normalizationjobs;


import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.ishan.smartcampus.utilities.CommonUtilities;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class NormalizingStudentData {
	
	public static class Map extends Mapper<LongWritable,Text,Text,NullWritable>{
		@Override
		public void map(LongWritable key, Text value,
				Context context)
				throws IOException,InterruptedException {
			
			String line = value.toString();
			String[] table_fields = line.split(",");
		
			table_fields[12] = CommonUtilities.getAge(table_fields[12]); // Getting Age
			
			
			table_fields[4] = CommonUtilities.getTerm(table_fields[4]); // Getting Graduation Term
			
			line = "";
			for (String s:table_fields)
			{
				line = line + s + ","  ;
			}
			context.write(new Text(line), NullWritable.get());
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//JobConf conf = new JobConf(WordCount.class);
		Configuration conf= new Configuration();
		
		
		//conf.setJobName("mywc");
		Job job = new Job(conf,"NsD");
		
		job.setJarByClass(NormalizingStudentData.class);
		job.setMapperClass(Map.class);
		//job.setReducerClass(Reduce.class);
		
		//conf.setMapperClass(Map.class);
		//conf.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		

		Path outputPath = new Path(args[1]);
			
	        //Configuring the input/output path from the filesystem into the job
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			//deleting the output path automatically from hdfs so that we don't have delete it explicitly
			
		outputPath.getFileSystem(conf).delete(outputPath);
			
			//exiting the job only if the flag value becomes false
			
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}