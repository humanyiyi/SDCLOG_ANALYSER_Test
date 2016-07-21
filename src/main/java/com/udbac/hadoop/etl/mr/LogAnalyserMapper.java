package com.udbac.hadoop.etl.mr;

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
public class LogAnalyserMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private final Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    private int inputRecords, filterRecords, outputRecords;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        this.logger.debug("Analyse data of :" + value);
        try {
            // 解析日志
            Map<String, String> logmap = LogUtil.handleLog(value.toString());

            if (logmap.isEmpty()) {
                this.filterRecords++;
                return;
            }
            handleData(logmap, context);
        } catch (Exception e) {
            this.filterRecords++;
            this.logger.error("处理数据发出异常，数据:" + value, e);
        }
    }

    private void handleData(Map<String, String> logmap, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (Map.Entry<String,String> entry:logmap.entrySet()){
            if (StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())) {
                sb.append(entry.getValue()).append("|");
            }
        }

        context.write(NullWritable.get(),new Text(sb.toString().substring(0,sb.length()-1)));
    }

    public static void main(String[] args) {
        Configuration conf =new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.4.21:8020");
//		conf.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
        try {
            Job job = Job.getInstance(conf,"wc");
            FileSystem fs = FileSystem.get(conf);
            job.setJarByClass(LogAnalyserMapper.class);
            job.setMapperClass(LogAnalyserMapper.class);

            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path("/user/admin/test/input/test.log"));
            //output目录不允许存在。
            Path output=new Path("/user/admin/test/output");
            if(fs.exists(output)){
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            boolean f= job.waitForCompletion(true);
            if(f){
                System.out.println("job 执行成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
