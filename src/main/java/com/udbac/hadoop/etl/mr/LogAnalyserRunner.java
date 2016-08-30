package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.CombinationKey;
import com.udbac.hadoop.common.DefinedComparator;
import com.udbac.hadoop.common.DefinedGroupSort;
import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.net.URI;

/**
 * Created by root on 2016/7/22.
 */
public class LogAnalyserRunner implements Tool {
    private static Logger logger = Logger.getLogger(LogAnalyserRunner.class);
    private Configuration conf = new Configuration();
    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new LogAnalyserRunner(), args);
        } catch (Exception e) {
            logger.info("执行JOB异常", e);
            throw new RuntimeException(e);
        }
    }
    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public void setConf(Configuration configuration) {
//        conf.set("mapred.jar", "");
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        String inputArgs[] = new GenericOptionsParser(conf, strings).getRemainingArgs();
        if (inputArgs.length != 2) {
            System.err.println("\"Usage:<in><out1>/n\"");
            System.exit(2);
        }
        String inputPath = inputArgs[0];
        String outputPath1 = inputArgs[0] + "/mr1out";
        String outputPath2 = inputArgs[1];
        conf.set(SDCLogConstants.RUNNING_DATE_PARAMES, TimeUtil.getYesterday());
        conf.set("inputPath", inputPath);

        Job job1 = Job.getInstance(conf, "LogAnalyserMap");
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
//        TextInputFormat.setInputPathFilter(job1,TextPathFilter.class);
        TextInputFormat.addInputPath(job1, new Path(inputPath));
        TextOutputFormat.setOutputPath(job1, new Path(outputPath1));

        Job job2 = Job.getInstance(conf, "SessionBuildMapper");
        job2.addCacheFile(new URI("/user/root/lib/qqwry.dat"));
        job2.setJarByClass(LogAnalyserRunner.class);
        job2.setMapperClass(SessionBuildMapper.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);
        //作业2加入控制容器
        ControlledJob ctrljob2 = new ControlledJob(conf);
        ctrljob2.setJob(job2);

        //意思为job2的启动，依赖于job1作业的完成
        ctrljob2.addDependingJob(ctrljob1);
        //输入路径是上一个作业的输出路径
        TextInputFormat.addInputPath(job2, new Path(outputPath1));
        TextOutputFormat.setOutputPath(job2, new Path(outputPath2));
        //LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
        //主的控制容器，控制上面的总的两个子作业
        JobControl jobCtrl = new JobControl("myctrl");
        //添加到总的JobControl里，进行控制
        jobCtrl.addJob(ctrljob1);
        jobCtrl.addJob(ctrljob2);
        //在线程启动，记住一定要有这个
        Thread t = new Thread(jobCtrl);
        t.start();

        FileSystem fs = FileSystem.get(conf);
        Path midoutput = new Path(outputPath1);

        while (true) {
            if (jobCtrl.allFinished()) {
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                fs.delete(midoutput, true);
                return 0;
            }
            if (jobCtrl.getFailedJobList().size() > 0) {
                System.out.println(jobCtrl.getFailedJobList());
                jobCtrl.stop();
                fs.delete(midoutput, true);
                return 1;
            }
        }
    }
}
