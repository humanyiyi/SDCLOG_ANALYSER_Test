package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.CombinationKey;
import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.entity.SDCLog;
import com.udbac.hadoop.etl.util.LogUtil;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 分析初始SDC日志的Mapper
 */
public class LogAnalyserMapper extends Mapper<LongWritable, Text, CombinationKey, Text> {
    private final Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    private CombinationKey combinationKey = new CombinationKey();
    private String inputPath = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        inputPath = configuration.get("inputPath");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            SDCLog sdcLog = LogUtil.handleLog(value.toString());
            if (null==sdcLog) {
                return;
            }
            combinationKey.setFirstKey(sdcLog.getUriQuery().get(SDCLogConstants.LOG_QUERY_NAME_DEVICEID));
            combinationKey.setSecondKey(TimeUtil.timeToInt(sdcLog.getTime()));
            if (inputPath.contains(sdcLog.getDate())) {
                context.write(combinationKey, new Text(sdcLog.toString()));
            }
        } catch (Exception e) {
            this.logger.error("处理SDCLOG出现异常，数据:" + value);
        }
    }

}
