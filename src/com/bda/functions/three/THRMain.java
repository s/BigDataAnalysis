package com.bda.functions.three;


import com.bda.functions.WholeFileInputFormat;
import com.bda.functions.onetwofour.OTFMapper;
import com.bda.functions.onetwofour.OTFReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import static com.bda.functions.Constants.*;

public class THRMain {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 1) {
            System.err.println("Usage: wordcount <in> [<in>...] <out> <function name>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf);

        job.setJarByClass((Class)THRMain.class);
        job.setMapperClass((Class)THRMapper.class);
        job.setCombinerClass((Class)THRReducer.class);
        job.setReducerClass((Class)THRReducer.class);

        job.setInputFormatClass(WholeFileInputFormat.class);

        job.setNumReduceTasks(1);
        job.setOutputKeyClass((Class)Text.class);
        job.setOutputValueClass((Class)IntWritable.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(baseURIString+dataDirectory));

        FileOutputFormat.setOutputPath((Job)job, (Path)new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
