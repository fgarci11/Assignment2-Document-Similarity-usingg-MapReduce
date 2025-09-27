package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class DocSizeMapper extends Mapper<Object, Text, Text, Text> {

    private static final Pattern NON_ALPHABETIC = Pattern.compile("[^a-zA-Z\\s]");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        
        int firstSpace = line.indexOf(' '); 
        if (firstSpace == -1) return; // If there's no space, skip the document (invalid input)

        //docId.set(line.substring(0, firstSpace)); // More efficient than regex splitting
        String docId = line.substring(0, firstSpace);
        String text = line.substring(firstSpace + 1);

        // Clean text consistently
        // text = text.replaceAll("[^a-zA-Z\\s]", " ").toLowerCase(); 
        text = NON_ALPHABETIC.matcher(text).replaceAll(" ").toLowerCase();
        StringTokenizer tokenizer = new StringTokenizer(text);

        while (tokenizer.hasMoreTokens()) {
            //word.set(tokenizer.nextToken());
            String word = tokenizer.nextToken();
            if (word.getLength() > 0) {
                // Key: Document ID, Value: Word
                // New object choice for to ensure correct serialization at the expense of garbage collection
                //context.write(new Text(docId), new Text(word)); 
                // Hadoop's context.write only accepts Writable types, so cast String to Text
                context.write(new Text(docId), new Text(word)); 
            }
        }
    }
}
