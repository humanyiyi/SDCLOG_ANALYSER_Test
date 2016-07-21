package com.udbac.hadoop.etl.util;

import com.udbac.hadoop.common.SDCLogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2016/7/13.
 */
public class LogUtil {
    private static final Logger logger = Logger.getLogger(LogUtil.class);

    public static Map<String, String> handleLog(String logText) {
        Map<String, String> logmap = new HashMap<String, String>();
        if (StringUtils.isNotBlank(logText)) {
            String splits[] = logText.split(SDCLogConstants.LOG_SEPARTIOR);
            if (splits.length == 15) {
                logmap.put(SDCLogConstants.LOG_COLUMN_NAME_Date, splits[0]);
                logmap.put(SDCLogConstants.LOG_COLUMN_NAME_Time, splits[1]);
                int index = logText.indexOf(" ");
//                if (index > -1) {
//                    String uriBody = splits[7];
//                    handleUriBody(uriBody, logmap);
//                }
            }
        }
        return logmap;
    }

    private static void handleUriBody(String uriBody, Map<String, String> logmap) {
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
                    } catch (UnsupportedEncodingException e) {
//                        logger.warn("解码操作出现异常", e);
                        e.printStackTrace();
                    }
                    if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)&& key.equals("wt.event")) {
                                logmap.put(key, value);
                    }
                }
            }
        }
    }
}
