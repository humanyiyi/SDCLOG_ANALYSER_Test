package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.util.IPSeekerExt;
import com.udbac.hadoop.util.TimeUtil;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/1/5.
 */
public class LogParserUtil {
    private final static Logger logger = Logger.getLogger(LogParserUtil.class);
    private static IPSeekerExt ipSeekerExt = new IPSeekerExt();

    public static Map<String, String> handleLog(String[] tokens) {
        Map<String, String> logMap = new HashMap<>();
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_Date_Time, TimeUtil.handleTime(tokens[0] + " " + tokens[1]));
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CS_USERNAME, tokens[3]);
        logMap.put(SDCLogConstants.LOG_COLUMN_CS_HOST, tokens[4]);
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CSMETHOD, tokens[5]);
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CSURISTEM, tokens[6]);
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_SCSTATUS, tokens[8]);
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_DCSID, tokens[14]);
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
            logMap.put(SDCLogConstants.LOG_COLUMN_NAME_OS_NAME, userAgent.getOperatingSystem().getName());
            logMap.put(SDCLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, userAgent.getBrowser().getName());
        }
    }

    private static void handleIP(Map<String, String> logMap, String ip) {
        if (StringUtils.isNotBlank(ip) && ip.length() > 8) {
            IPSeekerExt.RegionInfo info = ipSeekerExt.analyticIp(ip);
            if (null!=info) {
                logMap.put(SDCLogConstants.LOG_COLUMN_NAME_COUNTRY, info.getCountry());
                logMap.put(SDCLogConstants.LOG_COLUMN_NAME_PROVINCE, info.getProvince());
                logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CITY, info.getCity());
            }
        }
    }

}
