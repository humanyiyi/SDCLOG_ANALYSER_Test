package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by root on 2016/7/22.
 */
public class LogAnalyserRunner implements Tool {
    private static final Logger logger = Logger.getLogger(LogAnalyserRunner.class);
    private Configuration conf = new Configuration();


    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new LogAnalyserRunner(), args);
        } catch (Exception e) {
            logger.error("执行日志解析job异常", e);
            throw new RuntimeException(e);
        }
    }
    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration configuration) {
//        conf.set("fs.defaultFS", "hdfs://node11:8020");
//        conf.set("yarn.resourcemanager.hostname", "node11");
//        conf.set("hbase.zookeeper.quorum", "node11,node12,node13");
//        this.conf = HBaseConfiguration.create(conf);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        String inputArgs[] = new GenericOptionsParser(conf, strings).getRemainingArgs();
        if (inputArgs.length != 2) {
            System.err.println("\"Usage:<in> <out> <date1> <date2>/n\"");
            System.exit(2);
        }

        String inputPath = inputArgs[0];
        String outputPath = inputArgs[1];
        conf.set(SDCLogConstants.RUNNING_DATE_PARAMES, TimeUtil.getYesterday());

        Job job = Job.getInstance(conf, "LogAnalyserMapReduce");
        job.setJarByClass(LogAnalyserRunner.class);
        job.setMapperClass(LogAnalyserMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
//        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPathFilter(job, TextPathFilter.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true) ? 0 : -1;
    }

    static class TextPathFilter extends Configured implements PathFilter {
        @Override
        public boolean accept(Path path) {
            String date1 = getConf().get(SDCLogConstants.RUNNING_DATE_PARAMES);
            try {
                FileSystem fs = FileSystem.get(getConf());
                FileStatus fileStatus = fs.getFileStatus(path);

                if (fileStatus.isDirectory()) {
                    String name=path.getName();
                    if (TimeUtil.isValidateRunningDate(name)) {//文件夹名字 XXXXXXXX
                        if (name.equals(date1)) {//目录名字是否为当天日期 XXXXXXXX
                            return true;
                        }
                    } else if ("data".equals(name)) {
                        return true;
                    }
                }
                if (fileStatus.isFile()) {
                    return true;
                }

            } catch (IOException e) {
                logger.error("Format path error.");
            }
            return false;
        }
    }

}
