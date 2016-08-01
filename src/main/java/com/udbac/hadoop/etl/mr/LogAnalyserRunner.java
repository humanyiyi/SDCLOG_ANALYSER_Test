package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.*;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
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
        conf.set("fs.defaultFS", "hdfs://hadoop-01:8020");
        conf.set("yarn.resourcemanager.hostname", "hadoop-01");
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        String inputArgs[] = new GenericOptionsParser(conf, strings).getRemainingArgs();
        if (inputArgs.length != 3) {
            System.err.println("\"Usage:<in><out1><out2>/n\"");
            System.exit(2);
        }
        String inputPath = inputArgs[0];
        String outputPath1 = inputArgs[1];
        String outputPath2 = inputArgs[2];
        conf.set(SDCLogConstants.RUNNING_DATE_PARAMES, TimeUtil.getYesterday());

        Job job1 = Job.getInstance(conf, "LogAnalyserMapReduce");
        job1.setJarByClass(LogAnalyserRunner.class);
        job1.setMapperClass(LogAnalyserMapper.class);
        job1.setMapOutputKeyClass(CombinationKey.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setSortComparatorClass(DefinedComparator.class);
        job1.setGroupingComparatorClass(DefinedGroupSort.class);
        job1.setNumReduceTasks(1);

        //加入控制容器
        ControlledJob ctrljob1 = new ControlledJob(conf);
        ctrljob1.setJob(job1);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
//        FileInputFormat.setInputDirRecursive(job, true);
//        FileInputFormat.setInputPathFilter(job, TextPathFilter.class);
        FileOutputFormat.setOutputPath(job1, new Path(outputPath1));

        Job job2 = new Job(conf, "Join2");
        job2.setJarByClass(LogAnalyserRunner.class);
        job2.setMapperClass(EndAnalyseMapper.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);

        //作业2加入控制容器
        ControlledJob ctrljob2 = new ControlledJob(conf);
        ctrljob2.setJob(job2);

        //意思为job2的启动，依赖于job1作业的完成
        ctrljob2.addDependingJob(ctrljob1);
        //输入路径是上一个作业的输出路径，因此这里填args[1]
        FileInputFormat.addInputPath(job2, new Path(outputPath1));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath2));
        //主的控制容器，控制上面的总的两个子作业
        JobControl jobCtrl = new JobControl("myctrl");
        //添加到总的JobControl里，进行控制
        jobCtrl.addJob(ctrljob1);
        jobCtrl.addJob(ctrljob2);

        //在线程启动，记住一定要有这个
        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {
            if (jobCtrl.allFinished()) {//如果作业成功完成，就打印成功作业的信息
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }
        return job1.waitForCompletion(true) && job2.waitForCompletion(true) ? 0 : -1;
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
