package com.example;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocumentSimilarityMapper extends Mapper<Text, Text, Text, Text> {

    private Text word = new Text();
    private Text docId = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // Split document ID from document
        String[] parts = value.toString().split("\\s+", 2);
        if (parts.length < 2){
            throw new IOException("Invalid input format: expected at least two parts, found " + parts.length);
            // return
        }

        String documentId = parts[0];
        String documentContent = parts[1].toLowerCase().replaceAll("[^a-z0-9\\s]", ""); // Lowercase + strip punctuation
        docId.set(documentId); 

        StringTokenizer itr = new StringTokenizer(documentContent);
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, docId);
        }
    }
}
