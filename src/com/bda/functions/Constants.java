package com.bda.functions;

import org.apache.hadoop.io.IntWritable;

public class Constants {
    public static final String baseURIString = "hdfs://localhost:9000";
    public static final String dataDirectoryName = "data";
    public static final String dataDirectory = "/user/hadoop/"+dataDirectoryName;
    public static final IntWritable intWritableOne = new IntWritable(1);
    public static final Integer numberOfWordsToTake = 10;
}
