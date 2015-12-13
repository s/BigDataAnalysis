package com.bda.functions.onetwofour;

import static com.bda.functions.Constants.*;
import com.bda.functions.WholeFileInputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OTFMain {

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

        job.setJarByClass((Class)OTFMain.class);
        job.setMapperClass((Class)OTFMapper.class);
        job.setCombinerClass((Class)OTFReducer.class);
        job.setReducerClass((Class)OTFReducer.class);

        job.setInputFormatClass(WholeFileInputFormat.class);

        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(baseURIString+dataDirectory));

        FileOutputFormat.setOutputPath((Job)job, (Path)new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

