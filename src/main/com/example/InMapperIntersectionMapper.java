package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.*;

public class InMapperIntersectionMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final Pattern NON_ALPHABETIC = Pattern.compile("[^a-zA-Z\\s]");

    // Store: <Word, List of DocIDs that contain it>
    // This accumulates the local inverted index during the map phase
    private Map<String, List<String>> localInvertedIndex = new HashMap<>();
    
    // We use IntWritable for the value to align with the final Reducer's summation
    private final IntWritable ONE = new IntWritable(1); 
    
    @Override
    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString();
        int firstSpace = line.indexOf(' ');
        if (firstSpace == -1) return;

        String docId = line.substring(0, firstSpace);
        //String text = line.substring(firstSpace + 1).replaceAll("[^a-zA-Z\\s]", " ").toLowerCase();
        String text = NON_ALPHABETIC.matcher(text).replaceAll(" ");

        StringTokenizer tokenizer = new StringTokenizer(text);

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            if (word.length() > 0) {
                // Build the local inverted index: Word -> [Doc1, Doc2, ...]
                localInvertedIndex.computeIfAbsent(word, k -> new ArrayList<>()).add(docId);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // --- This is the In-Mapper Combining/Reduction Step ---
        
        // 1. Iterate over every word collected locally
        for (Map.Entry<String, List<String>> entry : localInvertedIndex.entrySet()) {
            List<String> docList = entry.getValue();

            // Remove duplicate docIDs in this word’s list (handle repeated words within a document)
            Set<String> uniqueDocs = new HashSet<>(docList);
            // Cast the Set of uniqueDocs to ArrayList for iterability
            List<String> uniqueDocList = new ArrayList<>(uniqueDocs);

            // 3. Generate all unique document pairs for this word
            for (int i = 0; i < uniqueDocList.size(); i++) {
                for (int j = i + 1; j < uniqueDocList.size(); j++) {
                    String docA = uniqueDocList.get(i);
                    String docB = uniqueDocList.get(j);
                    
                    // Ensure canonical order for the key: Doc1,Doc2
                    String pairKey = (docA.compareTo(docB) < 0) ? docA + "," + docB : docB + "," + docA;
                    
                    // Output: <DocPair, 1>
                    // The framework will automatically group and sum these locally, 
                    // or the JaccardReducer will sum them after the Shuffle.
                    context.write(new Text(pairKey), ONE); 
                }
            }
        }
    }
}
