package com.jamie.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TableDriver{
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job =  Job.getInstance(conf);

        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);

        job.setMapOutputKeyClass(TableBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI("file:///D:/hadoop/hdfs-learn/HadoopLearn/src/main/resources/TableMapJoin/pd.txt"));
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path("src/main/resources/TableMapJoin/Order.txt"));
        FileOutputFormat.setOutputPath(job,new Path("src/main/resources/TableMapJoin/Output"));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }



}
