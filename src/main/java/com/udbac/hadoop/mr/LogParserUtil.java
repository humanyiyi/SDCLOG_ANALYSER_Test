package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.util.IPv4Handler;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/5.
 */
public class LogParserUtil {
    private final static Logger logger = Logger.getLogger(LogParserUtil.class);

    public static SplitValueBuilder handleLog(String[] tokens,String... keys){
        SplitValueBuilder svb = new SplitValueBuilder();
        for (String key:keys)
        svb.add(handleLogMap(tokens).get(key));
        return  svb;
    }
    public static Map<String, String> handleLogMap(String[] tokens) {
        Map<String, String> logMap = new HashMap<>();
        logMap.put(LogConstants.LOG_COLUMN_DATETIME, TimeUtil.handleTime(tokens[0] + " " + tokens[1]));
        logMap.put(LogConstants.LOG_COLUMN_USERNAME, tokens[3]);
        logMap.put(LogConstants.LOG_COLUMN_HOST, tokens[4]);
        logMap.put(LogConstants.LOG_COLUMN_METHOD, tokens[5]);
        logMap.put(LogConstants.LOG_COLUMN_URISTEM, tokens[6]);
        logMap.put(LogConstants.LOG_COLUMN_STATUS, tokens[8]);
        logMap.put(LogConstants.LOG_COLUMN_DCSID, tokens[14]);
        handleIP(logMap, tokens[2]);
        handleQuery(logMap,tokens[7]);
        handleUA(logMap, tokens[11]);
        return logMap;
    }

    private static void handleQuery(Map<String, String> logMap, String query) {
        String[] uriQuerys = StringUtils.split(query, SDCLogConstants.QUERY_SEPARTIOR);
        for (String uriQuery : uriQuerys) {
            String[] uriitems = StringUtils.split(uriQuery, "=");
            if (uriitems.length == 2) {
                try {
                    uriitems[1] = URLDecoder.decode(uriitems[1], "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    logger.error("url解析异常" + e);
                    e.printStackTrace();
                }
                logMap.put(uriitems[0], uriitems[1]);
            }
        }
    }

    private static void handleUA(Map<String, String> logMap, String usString) {
        if (StringUtils.isNotBlank(usString)) {
            UserAgent userAgent = UserAgent.parseUserAgentString(usString);
            logMap.put(LogConstants.UA_OS_NAME, userAgent.getOperatingSystem().getName());
            logMap.put(LogConstants.UA_BROWSER_NAME, userAgent.getBrowser().getName());
        }
    }

    private static void handleIP(Map<String, String> logMap, String ip)  {
        if (StringUtils.isNotBlank(ip) && ip.length() > 8) {

            String[] info = new String[0];
            try {
                info = IPv4Handler.getArea(ip);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (null!=info) {
                logMap.put(LogConstants.REGION_PROVINCE, info[0]);
                logMap.put(LogConstants.REGION_CITY, info[1]);
                logMap.put(LogConstants.LOG_COLUMN_IPCODE, info[2]);
            }
        }
    }

}
