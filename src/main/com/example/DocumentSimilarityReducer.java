package com.example;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DocumentSimilarityReducer extends Reducer<Text, Text, Text, Text> {
    
    // Store intersection counts
    private Map<String, Integer> intersectionMap = new HashMap<>();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Each key is a word, each value is a list of docIDs containing that word
        // Collect unique documents for this word
        // // Set<String> docs = new HashSet<>();
        // // for (Text val : value) {
        // //     docs.add(val.toString());
        // // }
        // x actually I think this part above is really necessary

        // Count unique words per doc
        Set<String> uniqueWords = new HashSet<>();
        for (Text v : values) {
            uniqueWords.add(v.toString());
        }
        context.write(key, new Text(String.valueOf(uniqueWords.size())));

        // Generate all unique pairs of documents containing this word
        List<String> docList = new ArrayList<>(docs);
        for (int i = 0; i < docList.size(); i++) {
            for (int j = i + 1; j < docList.size(); j++) {
                String pair = docList.get(i) + "," + docList.get(j);
                intersectionMap.put(pair, intersectionMap.getOrDefault(pair, 0) + 1);
            }
        }


        // Later: expand to compute intersections/unions and Jaccard similarity
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Here you would normally join with doc sizes.
        // For now, just emit intersection counts as debug.
        for (Map.Entry<String, Integer> entry : intersectionMap.entrySet()) {
            context.write(new Text(entry.getKey()), new Text("Intersection: " + entry.getValue()));
        }
    }
}