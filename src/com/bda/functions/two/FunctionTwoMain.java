package com.bda.functions.two;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class FunctionTwoMain {

    private static final String baseURIString = "hdfs://localhost:9000";
    private static final String rootDirectory = "/user/said/Data/";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        Walker walker = new Walker(baseURIString, conf);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        job.setJarByClass((Class)FunctionTwoMain.class);
        job.setMapperClass((Class)FunctionTwoMapper.class);
        job.setCombinerClass((Class)FunctionTwoReducer.class);
        job.setReducerClass((Class)FunctionTwoReducer.class);

        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);

        Path[] finalInputPaths = walker.walkFromRoot(rootDirectory);

        FileInputFormat.setInputPaths(job, finalInputPaths);
        FileOutputFormat.setOutputPath((Job)job, (Path)new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}