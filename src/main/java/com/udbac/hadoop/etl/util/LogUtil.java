package com.udbac.hadoop.etl.util;

import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.entity.SDCLog;
import com.udbac.hadoop.util.UserAgentUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by root on 2016/7/13.
 */
public class LogUtil {
    private static final Logger logger = Logger.getLogger(LogUtil.class);
    private static IPSeekerExt ipSeekerExt = new IPSeekerExt();

    public static SDCLog handleLog(String logText) {
        SDCLog sdcLog = new SDCLog();
        if (StringUtils.isNotBlank(logText)) {
            String splits[] = logText.split(SDCLogConstants.LOG_SEPARTIOR);
            if (splits.length == 15) {
                String[] datetime = handleTime(splits[0] + " " + splits[1]).split(" ");
                sdcLog.setDate(datetime[0]);
                sdcLog.setTime(datetime[1].replace(":", ""));
//                //处理IP
//                sdcLog.setcIp(splits[2]);
//                handleIP(sdcLog);
//                //处理浏览器信息
//                sdcLog.setCsUserAgent(splits[11]);
//                handleUserAgent(sdcLog);
                sdcLog.setcIp("IP地址");
                sdcLog.setCsUserAgent("浏览器");
//                sdcLog.setsIp("服务器地址");
//                sdcLog.setCsUriStem("REST");
                int index = logText.indexOf(" ");
                if (index > -1) {
                    String uriBody = splits[7];
                    handleUriBody(uriBody, sdcLog);
                }
            }
        }
        return sdcLog;
    }

    private static String handleTime(String dateTime) {
        String realtime = null;
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date = dateFormat.parse(dateTime);
            calendar.setTime(date);
            calendar.add(Calendar.HOUR_OF_DAY, 7);
            calendar.add(Calendar.MINUTE, 59);
            calendar.add(Calendar.SECOND, 59);
            realtime = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(calendar.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return realtime;
    }

    private static void handleUriBody(String uriBody, SDCLog sdcLog) {
        HashMap<String, String> urimap = new HashMap<String, String>();
        if (StringUtils.isNotBlank(uriBody)) {
            String splits[] = uriBody.split("&");
            for (String param : splits) {
                if (StringUtils.isNotBlank(param)) {
                    int index = param.indexOf("=");
                    if (index < 0) {
//                        logger.info("无法解析参数：" + param + "， uri为:" + uriBody);
                        continue;
                    }
                    String key = null, value = null;
                    try {
                        key = param.substring(0, index);
                        value = URLDecoder.decode(param.substring(index + 1), "utf-8");
                        switch (key) {
                            case SDCLogConstants.LOG_EVENT_NAME_WTLOGIN:
                            case SDCLogConstants.LOG_EVENT_NAME_WTMENU:
                            case SDCLogConstants.LOG_EVENT_NAME_WTCART:
                            case SDCLogConstants.LOG_EVENT_NAME_WTUSER:
                            case SDCLogConstants.LOG_EVENT_NAME_WTSUC:
                            case SDCLogConstants.LOG_EVENT_NAME_WTPAY:
                                value = key.substring(3)+":"+value;
                                break;
                        }
                    } catch (UnsupportedEncodingException e) {
//                       logger.warn("解码操作出现异常", e);
                        e.printStackTrace();

                    }
                    if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                    	urimap.put(key, value);
                    }
                }
            }
            if (!urimap.containsKey(SDCLogConstants.LOG_QUERY_NAME_UTMSOURCE)) {
                urimap.put(SDCLogConstants.LOG_QUERY_NAME_UTMSOURCE, SDCLogConstants.DEFAULT_VALUE);
            }
            if (!urimap.containsKey(SDCLogConstants.LOG_QUERY_NAME_WTAVV)) {
                urimap.put(SDCLogConstants.LOG_QUERY_NAME_WTAVV, SDCLogConstants.DEFAULT_VALUE);
            }

            sdcLog.setUriQuery(urimap);
        }
    }

    private static void handleIP(SDCLog sdcLog) {
        if (StringUtils.isNotBlank(sdcLog.getcIp())) {
            String ip = sdcLog.getcIp();
            IPSeekerExt.RegionInfo info = ipSeekerExt.analyticIp(ip);
            if (info != null) {
                sdcLog.setcIp(info.getCountry()+","+info.getProvince()+","+info.getCity());
            }
        }
    }

    private static void handleUserAgent(SDCLog sdcLog) {
        if (StringUtils.isNotBlank(sdcLog.getCsUserAgent())) {
            UserAgentUtil.UserAgentInfo info = UserAgentUtil.analyticUserAgent(sdcLog.getCsUserAgent());
            if (info != null) {
                sdcLog.setCsUserAgent(info.toString());
            }
        }
    }
}
