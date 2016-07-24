package com.udbac.hadoop.etl.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by root on 2016/7/24.
 */
public class LogAnalyserReducer extends Mapper<LongWritable, Text, Text, Text> {
    private final Logger logger = Logger.getLogger(LogAnalyserMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
