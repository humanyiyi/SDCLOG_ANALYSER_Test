package com.udbac.hadoop.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.net.URI;


/**
 * Created by chaoslane@126.com on 2016/7/25.
 */
public class LogAnalyserRunner implements Tool {
    private static Logger logger = Logger.getLogger(LogAnalyserRunner.class);
    private Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new LogAnalyserRunner(), args);
        } catch (Exception e) {
            logger.error("执行JOB异常", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.set("fs.defaultFS", "hdfs://192.168.4.2:8022");

//        configuration.set("yarn.resourcemanager.hostname","192.168.4.3");
//        conf.set();
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        conf.addResource(LogAnalyserRunner.class.getResourceAsStream("/conf.xml"));
        String inputArgs[] = new GenericOptionsParser(conf, strings).getRemainingArgs();
        if (inputArgs.length != 3) {
            System.err.println("\"Usage:<inputPath><outputPath>/n\"");
            System.exit(2);
        }
        String inputPath = inputArgs[0];
        String outputPath = inputArgs[1];
        String ipPath = inputArgs[2];
//        String paraPath = inputArgs[3];

        Job job1 = Job.getInstance(conf, "LogAnalyser");
        TextInputFormat.addInputPath(job1, new Path(inputPath));
        TextOutputFormat.setOutputPath(job1, new Path(outputPath));

        job1.setJarByClass(LogAnalyserRunner.class);
        job1.setMapperClass(LogAnalyserMapper.class);

        job1.setMapOutputKeyClass(NullWritable.class);
        job1.addCacheFile(new URI(ipPath + "/udbacIPtransArea.csv"));
        job1.addCacheFile(new URI(ipPath + "/udbacIPtransSegs.csv"));
//        job1.addCacheFile(new URI(paraPath));

        job1.setNumReduceTasks(1);

        return job1.waitForCompletion(true) ? 0 : -1;
    }
}
