package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.util.QueryProperties;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import com.udbac.hadoop.util.SplitValueBuilder;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/10.
 */
public class LogAnalyserMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private final static Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    private Map<NullWritable,Text> mapoutput = new HashMap<>();
    private String inputPath = null;
    private  int filterRecords,inputRecords;

    protected void setup(Context context){
        Configuration configuration = context.getConfiguration();
        inputPath = configuration.get("inputPath_directory_name");
        mapoutput.clear();
    }

    protected void map(LongWritable key, Text value, Context context){
        try {
            String[] logTokens = StringUtils.split(value.toString(), SDCLogConstants.LOG_SEPARTIOR);
            if (logTokens.length == 15){
                SplitValueBuilder sdcLog = LogParserUtil.handleLog(logTokens, QueryProperties.query());
                context.write(NullWritable.get(), new Text(sdcLog.toString()));
            }
        }catch(Exception e) {
            filterRecords++;
            logger.error("处理SDCLOG出现的异常，数据：" + value + "\n");
            e.printStackTrace();
        }
    }

}
