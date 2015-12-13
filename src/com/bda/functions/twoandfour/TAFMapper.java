package com.bda.functions.twoandfour;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

import java.util.Properties;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import javax.mail.Session;

import com.bda.functions.Helper;
import com.bda.functions.Preprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TAFMapper extends org.apache.hadoop.mapreduce.Mapper<Text, BytesWritable, Object, IntWritable>{

    private static final IntWritable one = new IntWritable(1);
    private static final String regexpForNonAlphabeticalChars = "[^a-zA-Z0-9\\s]+";


    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

        Session s = Session.getDefaultInstance(new Properties());
        InputStream is = new ByteArrayInputStream(value.getBytes());
        MimeMessage message;
        String rawBody = "";

        try{
            message = new MimeMessage(s, is);
            rawBody = message.getContent().toString();

        } catch (MessagingException ex){

        }

        String preprocessedText = Preprocessor.cleanText(rawBody);

        String[] wordsOfText = preprocessedText.split(" ");
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        String personName = Helper.getPersonNameFromPath(filePathString);

        Configuration conf = context.getConfiguration();
        String functionName = conf.get("FunctionName");

        if(functionName.equals("BDAFunctionTwo")){
            for (String aString: wordsOfText){
                context.write(new Text(personName), one);
            }
        }else{
            for (String aString: wordsOfText){
                context.write(new Text(aString), one);
            }
        }


    }
}