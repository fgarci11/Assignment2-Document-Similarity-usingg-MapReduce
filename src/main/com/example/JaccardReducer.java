package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.filecache.DistributedCache;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JaccardReducer extends Reducer<Text, IntWritable, Text, Text> {
    
    // Stores: <DocumentID, UniqueWordCount> loaded from Job 1 output
    private final Map<String, Integer> docSizes = new HashMap<>();
    private final Text result = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        //Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf); 
        URI[] cacheFiles = context.getCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            //Path docSizeFilePath = cacheFiles[0]; 
            Path docSizeFilePath = new Path(cacheFiles[0].toString());

            try (BufferedReader br = new BufferedReader(new FileReader(docSizeFilePath.toString()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\s+"); // Split by DocID \t Count
                    if (parts.length == 2) {
                        docSizes.put(parts[0].trim(), Integer.parseInt(parts[1].trim()));
                    }
                }
            } catch (Exception e) {
                // Log failure to load cache file
                throw new IOException("Error reading Distributed Cache file: " + e.getMessage());
            }
        }
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        // Sum all received values (which are 1s or partial counts from the In-Mapper Combiner)
        int intersectionSize = 0; 
        for (IntWritable val : values) {
             intersectionSize += val.get(); 
        }

        // Split the key (DocA,DocB)
        String[] pair = key.toString().split(",");
        String docA = pair[0];
        String docB = pair[1];

        // Retrieve sizes from the loaded map (Job 1 output)
        int sizeA = docSizes.getOrDefault(docA, 0);
        int sizeB = docSizes.getOrDefault(docB, 0);

        // Calculate Union Size: |A ∪ B| = |A| + |B| - |A ∩ B|
        int unionSize = sizeA + sizeB - intersectionSize;

        double jaccardSimilarity = 0.0;
        if (unionSize > 0) {
            jaccardSimilarity = (double) intersectionSize / unionSize;
        }

        // Output: DocA,DocB Similarity: Score
        String formattedScore = String.format("%.2f", jaccardSimilarity);
        //result.set("Similarity: " + formattedScore);
        //context.write(key, result);
        context.write(key, new Text("Similarity: " + formattedScore));
    }
}
