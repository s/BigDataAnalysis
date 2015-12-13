package com.bda.functions;

import org.apache.hadoop.io.BytesWritable;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Helper {

    public Helper(){}

    public static String getPersonNameFromPath(String path){
        return path.split("Data/")[1].split("/")[0];
    }

    public static String[] getWordsOfAMailText(BytesWritable mailBody){

        Session s = Session.getDefaultInstance(new Properties());
        InputStream is = new ByteArrayInputStream(mailBody.getBytes());
        MimeMessage message;
        String rawBody = "";

        try{
            message = new MimeMessage(s, is);
            rawBody = message.getContent().toString();

        } catch (MessagingException ex){

        } catch (IOException ex){

        }
        String preprocessedText = Preprocessor.cleanText(rawBody);
        return preprocessedText.split(" ");
    }
}
