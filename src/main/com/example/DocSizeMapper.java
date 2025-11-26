package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class DocSizeMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final Pattern NON_ALPHABETIC = Pattern.compile("[^a-zA-Z\\s]");
    private final static IntWritable ONE = new IntWritable(1);
    private final Text outKey = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        int firstSpace = line.indexOf(' ');
        if (firstSpace == -1) return;

        String docId = line.substring(0, firstSpace);
        String text = line.substring(firstSpace + 1);
        text = NON_ALPHABETIC.matcher(text).replaceAll(" ").toLowerCase();

        Set<String> uniqueWords = new HashSet<>();
        StringTokenizer tokenizer = new StringTokenizer(text);

        while (tokenizer.hasMoreTokens()) {
            uniqueWords.add(tokenizer.nextToken());
        }

        outKey.set(docId);
        context.write(outKey, new IntWritable(uniqueWords.size()));
    }
}
