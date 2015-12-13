package com.bda.functions;

import java.util.regex.Matcher;

public class Preprocessor {

    private static final String urlToken = "%HTTPADDR%";
    private static final String emailToken = "%EMAILADDR%";
    private static final String numericToken = "%NUMERIC%";

    private static final String urlRegexp = "^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$";
    private static final String emailRegexp = "([^.@\\s]+)(\\.[^.@\\s]+)*@([^.@\\s]+\\.)+([^.@\\s]+)";
    private static final String nonAlphanumericRegexp = "[^a-zA-Z0-9\\s]";
    private static final String numericRegexp = "\\d{1,}";
    private static final String newlineRegexp = "\\s+";
    private static final String htmlRegexp = "\\<.*?\\>";

    public Preprocessor(){}

    public static String cleanText(String text){
        String cleanedText = "";

        cleanedText = tokenizeURLs(text);
        cleanedText = tokenizeEmailAdresses(cleanedText);
        cleanedText = tokenizeNumericCharacters(cleanedText);

        cleanedText = removeHTML(cleanedText);

        cleanedText = removeNonAlphanumericCharacters(cleanedText);
        cleanedText = removeSpecialCharacters(cleanedText);
        return cleanedText;
    }

    private static String removeHTML(String text){
        return text.replaceAll(htmlRegexp, " ");
    }

    private static String tokenizeURLs(String text){
        return text.replaceAll(urlRegexp, urlToken);
    }

    private static String tokenizeEmailAdresses(String text){
        return text.replaceAll(emailRegexp, emailToken);
    }

    private static String tokenizeNumericCharacters(String text){
        return text.replaceAll(numericRegexp, numericToken);
    }

    private static String removeNonAlphanumericCharacters(String text){
        return text.replaceAll(nonAlphanumericRegexp, " ");
    }

    private static String removeSpecialCharacters(String text){
        return text.trim().replaceAll(newlineRegexp, " ");
    }
}
