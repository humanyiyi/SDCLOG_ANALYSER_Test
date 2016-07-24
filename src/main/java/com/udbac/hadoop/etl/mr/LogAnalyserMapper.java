package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.entity.SDCLog;
import com.udbac.hadoop.etl.util.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

import java.util.Map;

/**
 * Log的Mapper
 */
public class LogAnalyserMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    private int inputRecords, filterRecords, outputRecords;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        this.logger.debug("Analyse data of :" + value);
        try {
            // 解析日志
            SDCLog sdcLog = LogUtil.handleLog(value.toString());

            if (null==sdcLog) {
                this.filterRecords++;
                return;
            }
            handleData(sdcLog, context);
        } catch (Exception e) {
            this.filterRecords++;
            this.logger.error("处理数据发出异常，数据:" + value, e);
        }
    }

    private void handleData(SDCLog sdcLog, Context context) throws IOException, InterruptedException {
        String key = sdcLog.getUriQuery().get(SDCLogConstants.LOG_QUERY_NAME_DEVICEID);
        String column = sdcLog.getDate()+"|"+sdcLog.getTime()+"|"+sdcLog.getcIp()+"|"+sdcLog.getCsUserAgent()+"|";
        String query =sdcLog.getUriQuery().get(SDCLogConstants.LOG_QUERY_NAME_UTMSOURCE)
                + "|"+sdcLog.getUriQuery().get(SDCLogConstants.LOG_QUERY_NAME_WTUTYPE)
                +"|"+sdcLog.getUriQuery().get(SDCLogConstants.LOG_QUERY_NAME_WTAVV)
                +"|"+sdcLog.getUriQuery().get(SDCLogConstants.LOG_QUERY_NAME_WTPOS)+"|";
        String event = sdcLog.getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTLOGIN)
                +","+sdcLog.getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTMENU)
                +","+sdcLog.getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTCART)
                +","+sdcLog.getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTUSER)
                +","+sdcLog.getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTSUC)
                +","+sdcLog.getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTPAY);
        context.write(new Text(key), new Text(column+query+event));

    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://node11:8020");
//        conf.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
        try {
            Job job = Job.getInstance(conf, "wc");
            FileSystem fs = FileSystem.get(conf);
            job.setJarByClass(LogAnalyserMapper.class);
            job.setMapperClass(LogAnalyserMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path("D:\\test\\test.log"));
            //output目录不允许存在。
            Path output = new Path("D:\\test\\output");
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("job 执行成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
