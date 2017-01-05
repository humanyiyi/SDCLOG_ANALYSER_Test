package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.SDCLogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 读入数据源的mapper
 */

public class LogAnalyserMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private final static Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    private int inputRecords, filterRecords, outputRecords;
    private CRC32 crc32 = new CRC32();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger.info("输入数据:" + this.inputRecords + "；输出数据:" + this.outputRecords + "；过滤数据:" + this.filterRecords);
    }

    /**
     * 读入一行数据 根据分割符切割后 为自定义key赋值
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        inputRecords++;
        try {
            String[] logtokens = StringUtils.split(value.toString(), SDCLogConstants.LOG_SEPARTIOR);
            if (logtokens.length != 15) {
                filterRecords++;
                return;
            }
            //处理日志方法
            Map<String, String> logMap = LogParserUtil.handleLog(logtokens);
            if (logMap.isEmpty()) {
                filterRecords++;
                return;
            }
            context.write(NullWritable.get(), new Text(logMap.toString()));
        } catch (Exception e) {
            filterRecords++;
            logger.error("处理SDCLOG出现异常，数据:" + value + "\n");
            e.printStackTrace();
        }
    }


    private void insertHbase(Map<String, String> logMap, Context context) throws IOException, InterruptedException {
        String dcsid = logMap.get(SDCLogConstants.LOG_COLUMN_NAME_DCSID);
        String date_time = logMap.get(SDCLogConstants.LOG_COLUMN_NAME_Date_Time);
        String rowkey = generateRowKey(dcsid, date_time);
//        for (Map.Entry<String, String> entry : logMap.entrySet()) {
//            if (StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())) {
//
//            }
//        }
//        context.write(NullWritable.get(), put);
    }

    private String generateRowKey(String dcsid, String date_time) {
        StringBuilder sb = new StringBuilder();
        sb.append(date_time).append("_");
        this.crc32.reset();
        if (StringUtils.isNotBlank(dcsid)) {
            this.crc32.update(dcsid.getBytes());
        }
        sb.append(this.crc32.getValue() % 100000000L);
        return sb.toString();
    }
}
