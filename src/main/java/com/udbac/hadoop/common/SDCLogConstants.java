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
     * 记录URL查询参数，此字段中存放SDK采集的自定义业务与用户行为数据
     */
    public static final String LOG_COLUMN_NAME_CsUriQuery = "cs-uri-query";
    /**
     * 用户唯一ID
     */
    public static final String LOG_QUERY_NAME_DEVICEID="wt.co_f";
    /**
     * 用户类型
     */
    public static final String LOG_QUERY_NAME_WTUTYPE="WT.branch";
    /**
     * APP下载渠道
     */
    public static final String LOG_QUERY_NAME_WTAVV="wt.avv";
    /**
     * 登录标识
     */
    public static final String LOG_QUERY_NAME_WTLOGIN="wt.login";
    /**
     * 浏览菜单标识
     */
    public static final String LOG_QUERY_NAME_WTMENU="wt.menu";
    /**
     * 用户信息页标识
     */
    public static final String LOG_QUERY_NAME_WTUSER="wt.user";
    /**
     * 购物车确认页标识
     */
    public static final String LOG_QUERY_NAME_WTCART="wt.cart";
    /**
     * 订单成功标识
     */
    public static final String LOG_QUERY_NAME_WTSUC="wt.suc";
    /**
     * 订单支付标识
     */
    public static final String LOG_QUERY_NAME_WTPAY="wt.pay";





    /**
     * 事件枚举类。指定事件的名称
     */
        public static enum EventEnum {
            WTLOGIN(1, "login view event", "wt.login"), // 登陆标识
            WTMENU(2, "menu view event", "wt.menu"), // 浏览菜单标识
            WTUSER(3, "user view event", "wt.user"), // 用户信息页标识
            WTCART(4, "cart event", "wt.cart"), // 购物车确认页标识
            WTSUC(5, "charge success event", "wt.suc"), // 订单成功标识
            WTPAY(6, "pay event", "wt.pay") // 订单支付标识
            ;

            public final int id; // id 唯一标识
            public final String name; // 名称
            public final String alias; // 别名，用于数据收集的简写

            private EventEnum(int id, String name, String alias) {
                this.id = id;
                this.name = name;
                this.alias = alias;
            }

            /**
             * 获取匹配别名的event枚举对象，如果最终还是没有匹配的值，那么直接返回null。
             *
             * @param alias
             * @return
             */
            public static EventEnum valueOfAlias(String alias) {
                for (EventEnum event : values()) {
                    if (event.alias.equals(alias)) {
                        return event;
                    }
                }
                return null;
            }
        }

    }
