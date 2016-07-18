package com.udbac.hadoop.common;

/**
 * Created by root on 2016/7/12.
 */
public class SDCLogConstants {
    /**
     * 日志分隔符
     */
    public static final String LOG_SEPARTIOR = " ";
    /**
     * 动作发生的日期
     */
    public static final String LOG_COLUMN_NAME_Date = "date";
    /**
     * 动作发生的时间（UTC标准时间）
     */
    public static final String LOG_COLUMN_NAME_Time = "time";
    /**
     * App客户端IP地址
     */
    public static final String LOG_COLUMN_NAME_Cip = "c-ip";
    /**
     * App服务器地址，原生App数据采集数据中此项为空。
     */
    public static final String LOG_COLUMN_NAME_Sip = "s-ip";
    /**
     *请求中使用的http方法，get/post
     */
    public static final String LOG_COLUMN_NAME_CsMethod = "cs-method";
    /**
     * 访问文件URI主体，原生App采集动作假名，HTML页面采集URI
     */
    public static final String LOG_COLUMN_NAME_CsUriStem = "cs-uri-stem";
    /**
     * 记录URL查询参数，此字段中存放SDK采集的自定义业务与用户行为数据
     */
    public static final String LOG_COLUMN_NAME_CsUriQuery = "cs-uri-query";
    /**
     * 记录http状态代码，200表示成功
     */
    public static final String LOG_COLUMN_NAME_ScStatus = "sc-status";
    /**
     * HTTP协议版本，如http/1.0
     */
    public static final String LOG_COLUMN_NAME_CsVersion = "cs-version";
    /**
     * 客户端浏览器、操作系统等情况，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_NAME_CsUserAgent = "cs(User-Agent)";
    /**
     * 客户端cookies内容，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_NAME_CsCookie = "cs(Cookie)";
    /**
     * 用户此动作时的前向链接，原生App采集中此数据为空。
     */
    public static final String LOG_COLUMN_NAME_CsReferer = "cs(Referer)";
    /**
     * 日期时间
     */
    public static final String LOG_QUERE_NAME_DCSDAT= "dcsdat";


}
