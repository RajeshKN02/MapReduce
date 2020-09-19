package com.rajesh.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text wordToken = new Text();
	
	 @Override
	public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		 
		 
		 StringTokenizer s = new StringTokenizer(value.toString());
		 while (s.hasMoreTokens()) {
			wordToken.set(s.nextToken());
			context.write(wordToken, new IntWritable(1));
			
			
		}
		 
	}
}
