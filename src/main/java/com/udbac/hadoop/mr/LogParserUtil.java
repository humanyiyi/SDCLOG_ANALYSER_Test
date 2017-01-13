package com.udbac.hadoop.mr;

import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.util.IPSeekerExt;
import com.udbac.hadoop.util.SplitValueBuilder;
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

    public static SplitValueBuilder handleLog(String[] tokens,String... keys){
        SplitValueBuilder svb = new SplitValueBuilder();
        for (String key:keys)
        svb.add(handleLogMap(tokens).get(key));
//        System.out.println(svb);
        return  svb;
    }
    private static Map<String, String> handleLogMap(String[] tokens) {
        Map<String, String> logMap = new HashMap<>();
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_Date_Time, TimeUtil.handleTime(tokens[0] + " " + tokens[1]));
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CS_USERNAME, tokens[3]);
        logMap.put(SDCLogConstants.LOG_COLUMN_CS_HOST, tokens[4]);
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CSMETHOD, tokens[5]);
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_CSURISTEM, tokens[6]);
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_SCSTATUS, tokens[8]);
        logMap.put(SDCLogConstants.LOG_COLUMN_NAME_DCSID, tokens[14]);
//        handleIP(logMap, tokens[2]);
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
//    public static void main(String[] Args){
//        String[] token = "2017-01-09 01:00:01 218.2.69.30 - - POST /page WT.a_cat=%E7%94%B5%E4%BF%A1&WT.a_nm=shapp&WT.a_pub=%E4%B8%AD%E5%9B%BD%E7%A7%BB%E5%8A%A8%E9%80%9A%E4%BF%A1%E6%9C%89%E9%99%90%E5%85%AC%E5%8F%B8&WT.av=android4.1.1&WT.cg_n=%E5%BF%AB%E6%8D%B7%E5%85%A5%E5%8F%A3&WT.cid=866479026029271&WT.co=yes&WT.co_f=LIRZPjGmimk&WT.ct=WIFI&WT.dc=CMCC&WT.dl=60&WT.dm=x600&WT.ets=1483923599270&WT.ev=click&WT.event=%E5%A5%97%E9%A4%90%E4%BD%99%E9%87%8F&WT.g_co=cn&WT.mobile=14782376101&WT.nv=%E5%BF%AB%E6%8D%B7%E5%85%A5%E5%8F%A3_SF001_2_1&WT.os=6.0&WT.pi=%E9%A6%96%E9%A1%B5&WT.si_n=%E5%A5%97%E9%A4%90%E4%BD%99%E9%87%8F&WT.si_x=20&WT.sr=1080x1920&WT.sys=button&WT.ti=pagename&WT.tz=8&WT.uc=United+States&WT.ul=English&WT.vt_sid=LIRZPjGmimk.1483922551343&WT.vtid=LIRZPjGmimk&WT.vtvs=1483922551343 200 - HTTP/1.0 WebtrendsClientLibrary/v1.3.0.52+(App_Android) - - dcsgss24ish8yws4p4dtu8q7g_9q7p\n".split(" ");
//        handleLog(token,QueryProperties.query());
//    }

}
