package com.example.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example.DocumentSimilarityMapper;
import com.example.DocumentSimilarityReducer;

public class DocumentSimilarityDriver {
    
    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: DocumentSimilarityDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "jaccard similarity");

        // Tells Hadoop which where to find compiled code and which logic classes to use
        job.setJarByClass(Controller.class);
        job.setMapperClass(DocumentSimilarityMapper.class);
        job.setReducerClass(DocumentSimilarityReducer.class);

        // Key-value output types; serving as the primary data structure for both input and output throughout the entire processing lifecycle
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input/output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}