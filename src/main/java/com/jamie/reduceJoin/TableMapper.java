package com.jamie.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper <LongWritable,Text, Text,TableBean> {

    private String filename;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if(filename.contains("Order")){
            String[] split = line.split(",");
            outK.set(split[1]); //pid
            outV.setId(split[0]);//id
            outV.setPid(split[1]);//pid
            outV.setPname("");//pname
            outV.setAmount(Integer.parseInt(split[2])); //amount
            outV.setFileFlag("Order");

        }else {
            String[] split = line.split(",");
            outK.set(split[0]); //pid
            outV.setId("");//id
            outV.setPid(split[0]);//pid
            outV.setPname(split[1]);//pname
            outV.setAmount(0); //amount
            outV.setFileFlag("pd");
        }
        context.write(outK,outV);

    }


}

