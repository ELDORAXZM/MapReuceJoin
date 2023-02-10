package com.jamie.mapJoin;

import com.ctc.wstx.util.StringUtil;
import com.jamie.reduceJoin.TableBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TableMapper extends Mapper<LongWritable, Text, TableBean, NullWritable> {

    private Map<String,String> pdMap = new HashMap<>();
    private Text text = new Text();
    @Override
    protected void setup(Mapper<LongWritable, Text, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        BufferedReader reader = new BufferedReader(new InputStreamReader(fis,"UTF-8"));

        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){
            String[] split = line.split(",");
            pdMap.put(split[0],split[1]);
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        TableBean tmpOrderBean = new TableBean();
        tmpOrderBean.setId(split[0]);
        tmpOrderBean.setPname(pdMap.get(split[1]));
        tmpOrderBean.setAmount(Integer.parseInt(split[2]));

        context.write(tmpOrderBean,NullWritable.get());




    }
}
