package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.util.TimeUtil;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 读入数据源的mapper
 */

public class LogAnalyserMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
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
            if (value.getLength() == 0) return;
            //处理日志方法
            Map<String, String> logMap = handleLog(value);
            if (logMap.isEmpty()) {
                filterRecords++;
                return;
            }
            insertHbase(logMap,context);
        } catch (Exception e) {
            filterRecords ++;
            logger.error("处理SDCLOG出现异常，数据:"+value+"\n"+ e);
            e.printStackTrace();
        }
    }

    private Map<String, String> handleLog(Text value) {
        Map<String, String> logMap = null;
        String[] tokens = StringUtils.split(value.toString(), SDCLogConstants.LOG_SEPARTIOR);
        if (tokens.length == 15) {
            logMap.put(SDCLogConstants.LOG_COLUMN_NAME_Date_Time, TimeUtil.handleTime(tokens[0] + tokens[1]));
            logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CIP, tokens[2]);
            logMap.put(SDCLogConstants.LOG_COLUMN_NAME_HOST, tokens[4]);
            String[] uriQuerys = StringUtils.split(tokens[7], SDCLogConstants.QUERY_SEPARTIOR);
            for (String uriQuery : uriQuerys) {
                String[] uriitems = StringUtils.split(uriQuery, "=");
                try {
                    uriitems[1] = URLDecoder.decode(uriitems[1], "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    logger.error("url解析异常"+e);
                    e.printStackTrace();
                }
                logMap.put(uriitems[0], uriitems[1]);
            }
            handleUA(logMap, tokens[11]);
            logMap.put(SDCLogConstants.LOG_COLUMN_NAME_DCSID, tokens[14]);
            return logMap;
        } else {
            return null;
        }
    }

    private Map<String, String> handleUA(Map<String, String> logMap, String ua) {
        if (StringUtils.isNotBlank(ua)) {
            UserAgent userAgent = UserAgent.parseUserAgentString(ua);
            logMap.put(SDCLogConstants.LOG_COLUMN_NAME_OS_NAME, userAgent.getOperatingSystem().toString());
            logMap.put(SDCLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, userAgent.getBrowser().toString());
            logMap.put(SDCLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, userAgent.getBrowserVersion().toString());
        }
        return logMap;
    }

    private void insertHbase(Map<String, String> logMap,Context context) throws IOException, InterruptedException {
        String dcsid = logMap.get(SDCLogConstants.LOG_COLUMN_NAME_DCSID);
        String date_time = logMap.get(SDCLogConstants.LOG_COLUMN_NAME_Date_Time);
        String rowkey = generateRowKey(dcsid, date_time);
        Put put = new Put(Bytes.toBytes(rowkey));
        for (Map.Entry<String, String> entry : logMap.entrySet()) {
            if (StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())) {
                put.add(Bytes.toBytes("CF1"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
            }
        }
        context.write(NullWritable.get(), put);
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
