package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DocSizeReducer extends Reducer<Text, Text, Text, IntWritable> {

    //private final IntWritable size = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        // A Set is used to count the unique words (values) for each docID (key)
        Set<String> uniqueWords = new HashSet<>();
        for (Text word : values) {
            uniqueWords.add(word.toString());
        }

        //size.set(uniqueWords.size());
        // Emit (DocID, unique word count)
        //context.write(key, size); 
        // New object choice for to ensure correct serialization at the expense of garbage collection
        context.write(key, new IntWritable(uniqueWords.size())); 
    }
}