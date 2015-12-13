package com.bda.functions.twoandfour;

import com.bda.functions.Walker;

import java.io.File;
import java.net.URI;
import java.util.Date;
import java.text.SimpleDateFormat;

import com.bda.functions.WholeFileInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TAFMain{

    private static final String baseURIString = "hdfs://localhost:9000";
    private static final String rootDirectory = "/user/said/";
    private static final String dataDirectory = "/user/said/Data/";
    private static final String outputBaseDirectory = "/user/said/Outputs/";

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 1) {
            System.err.println("Usage: wordcount <in> [<in>...] <out> <function name>");
            System.exit(2);
        }

        String functionName = otherArgs[0];
        conf.set("FunctionName", functionName);

        Job job = Job.getInstance(conf);
        FileSystem dfs = FileSystem.get(URI.create(baseURIString+rootDirectory), conf);

        job.setJarByClass((Class)TAFMain.class);
        job.setMapperClass((Class)TAFMapper.class);
        job.setCombinerClass((Class)TAFReducer.class);
        job.setReducerClass((Class)TAFReducer.class);

        job.setInputFormatClass(WholeFileInputFormat.class);

        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(baseURIString+dataDirectory));

        FileOutputFormat.setOutputPath((Job)job, (Path)new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

