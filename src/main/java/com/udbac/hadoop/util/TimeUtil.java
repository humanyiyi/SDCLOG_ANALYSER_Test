package com.udbac.hadoop.util;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * 时间控制工具类
 */
public class TimeUtil {
    private static final String DATE_FORMAT = "yyyy-MM-dd";

    private static final String TIME_FORMAT = "HH:mm:ss";

    /**
     * 获取昨日的日期格式字符串数据
     */
    public static String getYesterday() {
        return getYesterday(DATE_FORMAT);
    }


    /**
     * 获取对应格式的时间字符串
     */
    public static String getYesterday(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(calendar.getTime());
    }


    /**
     * 判断输入的参数是否是一个有效的时间格式数据
     */
    public static boolean isValidateRunningDate(String input) {
        Matcher matcher = null;
        boolean result = false;
        String regex = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
        if (input != null && !input.isEmpty()) {
            Pattern pattern = Pattern.compile(regex);
            matcher = pattern.matcher(input);
        }
        if (matcher != null) {
            result = matcher.matches();
        }
        return result;
    }


    /**
     * 将yyyy-MM-dd格式的时间字符串转换为时间戳
     */
    public static long parseStringDate2Long(String input) {
        return parseString2Long(input, DATE_FORMAT+" "+TIME_FORMAT);
    }


    /**
     * 将时间戳转换为yyyy-MM-dd格式的时间字符串
     */
    public static String parseLong2StringDate(long input) {
        return parseLong2String(input, DATE_FORMAT);
    }


    /**
     * 将时间戳转换为指定格式的字符串
     */
    public static String parseLong2String(long input, String pattern) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(input);
        return new SimpleDateFormat(pattern).format(calendar.getTime());
    }


    /**
     * 将指定格式的时间字符串转换为时间戳
     */
    public static long parseString2Long(String input, String pattern) {
        Date date;
        try {
            date = new SimpleDateFormat(pattern).parse(input);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return date.getTime();
    }


    /**
     * 将nginx服务器时间转换为时间戳，如果说解析失败，返回-1
     */
    public static long parseNginxServerTime2Long(String input) {
        Date date = parseNginxServerTime2Date(input);
        return date == null ? -1L : date.getTime();
    }


    /**
     * 将nginx服务器时间转换为date对象。如果解析失败，返回null
     * @param input 格式: 1449410796.976
     */
    public static Date parseNginxServerTime2Date(String input) {
        if (StringUtils.isNotBlank(input)) {
            try {
                long timestamp = Double.valueOf(Double.valueOf(input.trim()) * 1000).longValue();
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(timestamp);
                return calendar.getTime();
            } catch (Exception e) {
                // nothing
            }
        }
        return null;
    }


    /**
     * 获取time指定周的第一天的时间戳值
     */
    public static long getFirstDayOfThisWeek(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        cal.set(Calendar.DAY_OF_WEEK, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }


    /**
     * 给时间+8 hour 保证日志时间范围是1天内
     * @param dateTime date+" "+time
     * @return date+" "+time
     */
    public static String handleTime(String dateTime) {
        String realtime = null;
        AtomicReference<Calendar> calendar;
        calendar = new AtomicReference<>(Calendar.getInstance());
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT+" "+TIME_FORMAT);
        try {
            Date date = dateFormat.parse(dateTime);
            calendar.get().setTime(date);
            calendar.get().add(Calendar.HOUR_OF_DAY, 7);
            calendar.get().add(Calendar.MINUTE, 59);
            calendar.get().add(Calendar.SECOND, 59);
            realtime = (new SimpleDateFormat(DATE_FORMAT+" "+TIME_FORMAT)).format(calendar.get().getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return realtime;
    }
}
