package com.bda.functions.three;

import com.bda.functions.Helper;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class THRMapper extends Mapper<Text, BytesWritable, Object, IntWritable>{

    private static final IntWritable intWritableOne = new IntWritable(1);

    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

        String[] wordsOfText = Helper.getWordsOfAMailText(value);
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        String personName = Helper.getPersonNameFromPath(filePathString);

        String finalKey = "";
        for (String aString: wordsOfText){
            finalKey = personName + "_" + aString;
            context.write(new Text(finalKey), intWritableOne);
        }
    }
}
