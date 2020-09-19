package com.rajesh.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	private static final int MIN_ARGS = 2;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();

		try {
			String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (pathArgs.length < MIN_ARGS) {
				System.err.println("MR Project Usage : wordcount <input-path> [....] <output-path>");
				System.exit(2);
			}
			Job wcJob = Job.getInstance(conf, "MapReduce WordCount");
			wcJob.setJarByClass(WordCount.class);
			wcJob.setMapperClass(WordCountMapper.class);
			wcJob.setCombinerClass(WordCountReducer.class);
		    wcJob.setReducerClass(WordCountReducer.class);
			wcJob.setOutputKeyClass(Text.class);
			wcJob.setOutputValueClass(IntWritable.class);

			for (int i = 0; i < pathArgs.length - 1; ++i) {
				FileInputFormat.addInputPath(wcJob, new Path(pathArgs[i]));
			}

			FileOutputFormat.setOutputPath(wcJob, new Path(pathArgs[pathArgs.length - 1]));
			System.exit(wcJob.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}

	}

}
