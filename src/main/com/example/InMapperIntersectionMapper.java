// package com.example;

// import org.apache.hadoop.io.IntWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Mapper;
// import java.io.IOException;
// import java.util.*;
// import java.util.regex.Pattern;

// public class InMapperIntersectionMapper extends Mapper<Object, Text, Text, IntWritable> {

//     private static final Pattern NON_ALPHABETIC = Pattern.compile("[^a-zA-Z\\s]");

//         // Store docPair, partial count
//             private final Map<String, Integer> pairCounts = new HashMap<>();

//                 // Store: <Word, List of DocIDs that contain it>
//                     // This accumulates the local inverted index during the map phase
//                         private Map<String, List<String>> localInvertedIndex = new HashMap<>();
                            
//                                 // // We use IntWritable for the value to align with the final Reducer's summation
//                                     // private final IntWritable ONE = new IntWritable(1); 
                                        
//                                             @Override
//                                                 public void map(Object key, Text value, Context context) 
//                                                             throws IOException, InterruptedException {
                                                                    
//                                                                             String line = value.toString();
//                                                                                     int firstSpace = line.indexOf(' ');
//                                                                                             if (firstSpace == -1) return;

//                                                                                                     String docId = line.substring(0, firstSpace);
//                                                                                                             String text = line.substring(firstSpace + 1); //.replaceAll("[^a-zA-Z\\s]", " ").toLowerCase();
//                                                                                                                     text = NON_ALPHABETIC.matcher(text).replaceAll(" ").toLowerCase();

//                                                                                                                             StringTokenizer tokenizer = new StringTokenizer(text);

//                                                                                                                                     while (tokenizer.hasMoreTokens()) {
//                                                                                                                                                 String word = tokenizer.nextToken();
//                                                                                                                                                             if (word.length() > 0) {
//                                                                                                                                                                             // Build the local inverted index: Word -> [Doc1, Doc2, ...]
//                                                                                                                                                                                             localInvertedIndex.computeIfAbsent(word, k -> new ArrayList<>()).add(docId);
//                                                                                                                                                                                                         }
//                                                                                                                                                                                                                 }
//                                                                                                                                                                                                                     }

//                                                                                                                                                                                                                         private void incrementPairCount(String pairKey) {
//                                                                                                                                                                                                                                 Integer oldValue = pairCounts.get(pairKey);
//                                                                                                                                                                                                                                         if (oldValue == null) {
//                                                                                                                                                                                                                                                     pairCounts.put(pairKey, 1);
//                                                                                                                                                                                                                                                             } else {
//                                                                                                                                                                                                                                                                         pairCounts.put(pairKey, oldValue + 1);
//                                                                                                                                                                                                                                                                                 }
//                                                                                                                                                                                                                                                                                     }

//                                                                                                                                                                                                                                                                                         // Aggregate to partial sums
//                                                                                                                                                                                                                                                                                             private final Map<String, List<String>> wordToDocs = new HashMap<>();

//                                                                                                                                                                                                                                                                                                 @Override
//                                                                                                                                                                                                                                                                                                     protected void cleanup(Context context) throws IOException, InterruptedException {
//                                                                                                                                                                                                                                                                                                             // --- This is the In-Mapper Combining/Reduction Step ---
                                                                                                                                                                                                                                                                                                                    
//                                                                                                                                                                                                                                                                                                                             // 1. Iterate over every word collected locally
//                                                                                                                                                                                                                                                                                                                                     for (Map.Entry<String, List<String>> entry : localInvertedIndex.entrySet()) {
//                                                                                                                                                                                                                                                                                                                                                 List<String> uniqueDocList = entry.getValue();

//                                                                                                                                                                                                                                                                                                                                                             // // Remove duplicate docIDs in this word’s list (handle repeated words within a document)
//                                                                                                                                                                                                                                                                                                                                                                         // Set<String> uniqueDocs = new HashSet<>(docList);
//                                                                                                                                                                                                                                                                                                                                                                                     // // Cast the Set of uniqueDocs to ArrayList for iterability
//                                                                                                                                                                                                                                                                                                                                                                                                 // List<String> uniqueDocList = new ArrayList<>(uniqueDocs);

//                                                                                                                                                                                                                                                                                                                                                                                                             // 3. Generate all unique document pairs for this word
//                                                                                                                                                                                                                                                                                                                                                                                                                         for (int i = 0; i < uniqueDocList.size(); i++) {
//                                                                                                                                                                                                                                                                                                                                                                                                                                         for (int j = i + 1; j < uniqueDocList.size(); j++) {
//                                                                                                                                                                                                                                                                                                                                                                                                                                                             String docA = uniqueDocList.get(i);
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 String docB = uniqueDocList.get(j);
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         // Ensure canonical order for the key: Doc1,Doc2
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             String pairKey = (docA.compareTo(docB) < 0) ? docA + "," + docB : docB + "," + docA;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     // Output: <DocPair, 1>
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         // The framework will automatically group and sum these locally, 
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             // or the JaccardReducer will sum them after the Shuffle.
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 //////context.write(new Text(pairKey), ONE); 
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     //pairCounts.merge(pairKey, 1, Integer::sum);
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         incrementPairCount(pairKey);
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         }
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     }
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             }

//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     // Emit each pair once with its partial count
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             for (Map.Entry<String, Integer> entry : pairCounts.entrySet()) {
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 }
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     }
//                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     }
package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class InMapperIntersectionMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final Pattern NON_ALPHABETIC = Pattern.compile("[^a-zA-Z\\s]");

    // Store docPair, partial count
    private final Map<String, Integer> pairCounts = new HashMap<>();

    // Store: <Word, List of DocIDs that contain it>
    // This accumulates the local inverted index during the map phase
    // private Map<String, List<String>> localInvertedIndex = new HashMap<>();
    private Map<String, Set<String>> localInvertedIndex = new HashMap<>();

    // // We use IntWritable for the value to align with the final Reducer's summation
    // private final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        int firstSpace = line.indexOf(' ');
        if (firstSpace == -1) return;

        String docId = line.substring(0, firstSpace);
        String text = line.substring(firstSpace + 1); //.replaceAll("[^a-zA-Z\\s]", " ").toLowerCase();
        text = NON_ALPHABETIC.matcher(text).replaceAll(" ").toLowerCase();

        StringTokenizer tokenizer = new StringTokenizer(text);

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            if (word.length() > 0) {
                // Build the local inverted index: Word -> [Doc1, Doc2, ...]
                localInvertedIndex.computeIfAbsent(word, k -> new HashSet<>()).add(docId);//can probably be done with Set
            }
        }
    }

    private void incrementPairCount(String pairKey) {
        Integer oldValue = pairCounts.get(pairKey);
        if (oldValue == null) {
            pairCounts.put(pairKey, 1);
        } else {
            pairCounts.put(pairKey, oldValue + 1);
        }
    }

    // Aggregate to partial sums
    //private final Map<String, List<String>> wordToDocs = new HashMap<>();

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // --- This is the In-Mapper Combining/Reduction Step ---

        // 1. Iterate over every word collected locally
        for (Map.Entry<String, Set<String>> entry : localInvertedIndex.entrySet()) {
            Set<String> uniqueDocs = entry.getValue();

            if (uniqueDocs.size() > 1) {
                List<String> uniqueDocList = new ArrayList<>(uniqueDocs);
            }

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
                    //////context.write(new Text(pairKey), ONE);
                    //pairCounts.merge(pairKey, 1, Integer::sum);
                    incrementPairCount(pairKey);
                }
            }
        }

        // Emit each pair once with its partial count
        for (Map.Entry<String, Integer> entry : pairCounts.entrySet()) {
            context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
        }
    }
}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 