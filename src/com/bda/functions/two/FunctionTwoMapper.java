package com.bda.functions.two;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FunctionTwoMapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Object, IntWritable>{

    private static final IntWritable one = new IntWritable(1);
    private static final String regexpForNonAlphabeticalChars = "[^a-zA-Z0-9\\s]+";


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String rawText = value.toString();
        String alphabeticalString = rawText.replaceAll(regexpForNonAlphabeticalChars, "");

        String[] wordsOfLine = alphabeticalString.split(" ");
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();

        String personName = filePathString.split("Data/")[1].split("/")[0];

        for (String aString: wordsOfLine){
            context.write(new Text(personName), one);
        }

    }
}