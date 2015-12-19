package com.bda.functions.three;

import java.util.*;
import java.util.HashMap;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import static com.bda.functions.Constants.*;

public class THRReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Map<String, Map> allPersonsWordsMap = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values)
        {
            sum += val.get();
        }

        String personName = key.toString().split("_")[0];

        if (null == allPersonsWordsMap.get(personName)){
            Map<Text, IntWritable> aPersonsWordsMap = new HashMap<>();
            allPersonsWordsMap.put(personName, aPersonsWordsMap);
        }

        allPersonsWordsMap.get(personName).put(new Text(key), new IntWritable(sum));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int processedCount;

        for(String personName: allPersonsWordsMap.keySet()){
            processedCount = 0;
            Map<Text, IntWritable> sortedMap = sortByValues(allPersonsWordsMap.get(personName));

            for(Text key: sortedMap.keySet()){
                if (processedCount>=numberOfWordsToTake)
                    break;

                context.write(key, sortedMap.get(key));
                processedCount++;
            }
        }
    }

    /*
   * sorts the map by values. Taken from:
   * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
   */
    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
}

