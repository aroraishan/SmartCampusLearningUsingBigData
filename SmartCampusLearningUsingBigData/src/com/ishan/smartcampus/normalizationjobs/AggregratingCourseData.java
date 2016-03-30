package com.ishan.smartcampus.normalizationjobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.ishan.smartcampus.utilities.CommonUtilities;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class AggregratingCourseData {
	
	public static class Map extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		public void map(LongWritable key, Text value,
				Context context)
				throws IOException,InterruptedException {
			String line = value.toString();
			String[] table_fields = line.split("#");
			
					
					
				context.write(new Text(table_fields[0]), new Text(line.substring(line.indexOf('#') + 1, line.lastIndexOf('#'))));
		}
		
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,NullWritable>{
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException,InterruptedException {
			int sum=0;
			String academic_standing = "Good";
			int enrollment_term = 2172;
			HashMap<String, String> course_data = new HashMap<String, String>();
			Set<String> semester_total = new TreeSet<String>();
			// TODO Auto-generated method stub
			for(Text x: values)
			{
				String record = x.toString();
				if(enrollment_term > Integer.parseInt(record.split("#")[0].trim()))
				{
					enrollment_term = Integer.parseInt(record.split("#")[0].trim());
				}
				String course_term = record.split("#")[0].trim();
				if (Integer.parseInt(course_term) != 2162)
				{
					semester_total.add(course_term);
				}
				
				String course_name = record.split("#")[2].trim();
				System.out.println("ishan" + key.toString());
				System.out.println("arora" + x.toString());
				String course_grade = record.split("#")[7].trim();
				
				if (Integer.parseInt(course_term) != 2162 && course_grade !="")
				{
					if (!course_data.containsKey(course_term+course_name))
					{
						if (course_grade.equals("C-") || course_grade.equals("C") || course_grade.contains("D") || course_grade.contains("F") || course_grade.contains("IC") || course_grade.contains("WU") || course_grade.contains("NC"))
						{
							academic_standing = "Poor";
						}
						course_data.put(course_term + course_name, course_grade);
					}
					
				}
				
			}
			float average_unit = 0;
			try
			{
			average_unit = (course_data.size()*3)/semester_total.size();
			}
			catch(ArithmeticException e)
			{
				average_unit =  0;
			}
			System.out.println(course_data);
			System.out.println(course_data.values().getClass());
			String enrollment_date = CommonUtilities.getTerm(Integer.toString(enrollment_term));
			//context.write(key, new Text(enrollment_date));
			//context.write(key, new Text(Float.toString(average_unit)));
			String final_value = key.toString() + "," +  enrollment_date + "," + Float.toString(average_unit) + "," + academic_standing;
			context.write(new Text(final_value), NullWritable.get());
			
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//JobConf conf = new JobConf(WordCount.class);
		Configuration conf= new Configuration();
		
		
		//conf.setJobName("mywc");
		Job job = new Job(conf,"NcD");
		
		job.setJarByClass(NormalizingCourseData.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//conf.setMapperClass(Map.class);
		//conf.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
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