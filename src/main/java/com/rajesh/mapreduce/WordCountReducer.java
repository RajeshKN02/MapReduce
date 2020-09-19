package com.rajesh.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>	{
	
	private IntWritable count = new IntWritable();
	
	@Override
	public void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		
		int valueSum = 0;
		for (IntWritable val : arg1) {
			valueSum += val.get();
		}
		count.set(valueSum);
		arg2.write(arg0, count);
	}
}
