package com.bda.functions;

public class Helper {

    public Helper(){}

    public static String getPersonNameFromPath(String path){
        return path.split("Data/")[1].split("/")[0];
    }
}
