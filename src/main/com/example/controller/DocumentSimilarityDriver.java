package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.filecache.DistributedCache; 

import com.example.DocSizeMapper;
import com.example.DocSizeReducer;
import com.example.InMapperIntersectionMapper;
import com.example.JaccardReducer;

public class DocumentSimilarityDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: DocumentSimilarityDriver <input path> <temp output path> <final output path>");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]); // Input for both Job 1 and Job 2
        Path docSizesOutputPath = new Path(args[1] + "_doc_sizes"); // Output location for Job 1
        Path finalOutputPath = new Path(args[2]); // Output location for Job 2

        Configuration conf = new Configuration();

        // Job 1: Calculate document sizes (the number of unique words in a document)
        Job docSizesJob = Job.getInstance(conf, "doc sizes");
        docSizesJob.setJarByClass(DocumentSimilarityDriver.class);
        docSizesJob.setMapperClass(DocSizeMapper.class);
        docSizesJob.setReducerClass(DocSizeReducer.class);
        docSizesJob.setMapOutputKeyClass(Text.class); // DocID
        docSizesJob.setMapOutputValueClass(Text.class); // Word
        docSizesJob.setOutputKeyClass(Text.class); // DocID
        docSizesJob.setOutputValueClass(IntWritable.class); // Size
        FileInputFormat.addInputPath(docSizesJob, inputPath);
        FileOutputFormat.setOutputPath(docSizesJob, docSizesOutputPath);
        // The default number of reducers in Hadoop is 1, and this solution is tailored to that default
        docSizesJob.setNumReduceTasks(1);
        // Wait for Job 1 to complete before proceeding
        // Job 2 must run after Job 1 due to data dependency
        if (!docSizesJob.waitForCompletion(true)) {
            System.exit(1);
        }

        // Pass the document sizes (Job 1) output to the distributed cache so it can be used by the JaccardReducer
        Path docSizePartFile = new Path(docSizesOutputPath, "part-r-00000"); 
        // DistributedCache.addCacheFile(docSizePartFile.toUri(), conf);

        // Job 2: Intersection (in-mapper) combiner + jaccard calculation        
        Job jaccardJob = Job.getInstance(conf, "jaccard similarity (network optimized)");
        jaccardJob.addCacheFile(docSizePartFile.toUri());
        jaccardJob.setJarByClass(DocumentSimilarityDriver.class);
        // Mapper performs in-mapper combining (generates <word, docID> pairs and local sums)
        jaccardJob.setMapperClass(InMapperIntersectionMapper.class); 
        // Reducer performs final aggregation (summing partial counts) and jaccard calculation
        jaccardJob.setReducerClass(JaccardReducer.class);
        jaccardJob.setMapOutputKeyClass(Text.class); // DocPair as a Text object
        jaccardJob.setMapOutputValueClass(IntWritable.class); // Partial intersection count (local sum) as IntWritable
        jaccardJob.setOutputKeyClass(Text.class); // DocPair as a Text object
        jaccardJob.setOutputValueClass(Text.class); // Jaccard score as a Text object e.g. 'Similarity: 0.56'
        FileInputFormat.addInputPath(jaccardJob, inputPath);
        FileOutputFormat.setOutputPath(jaccardJob, finalOutputPath);
        System.exit(jaccardJob.waitForCompletion(true) ? 0 : 1);
    }
}