package com.bda.functions.onetwofour;

import java.io.IOException;

import com.bda.functions.Helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class OTFMapper extends Mapper<Text, BytesWritable, Object, IntWritable>{

    private static final IntWritable intWritableOne = new IntWritable(1);

    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

        String[] wordsOfText = Helper.getWordsOfAMailText(value);
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        String personName = Helper.getPersonNameFromPath(filePathString);

        Configuration conf = context.getConfiguration();
        String functionName = conf.get("FunctionName");

        if(functionName.equals("BDAFunctionOne")){
            context.write(new Text(personName), intWritableOne);
        }
        else if(functionName.equals("BDAFunctionTwo")){
            for (String aString: wordsOfText){
                context.write(new Text(personName), intWritableOne);
            }
        }
        else if(functionName.equals("BDAFunctionFour")){
            for (String aString: wordsOfText){
                context.write(new Text(aString), intWritableOne);
            }
        }


    }
}