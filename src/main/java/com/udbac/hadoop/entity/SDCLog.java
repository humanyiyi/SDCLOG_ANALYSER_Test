package com.udbac.hadoop.entity;

import com.udbac.hadoop.common.SDCLogConstants;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2016/7/22.
 */
public class SDCLog {
    //列
    private String date;
    private String time;
    private String cIp;
    private String sIp;
    private String csUriStem;
    private String csUserAgent;

    private Map<String, String> uriQuery;


    public Map<String, String> getUriQuery() {
        return uriQuery;
    }

    public void setUriQuery(Map<String, String> uriQuery) {
        this.uriQuery = uriQuery;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getcIp() {
        return cIp;
    }

    public void setcIp(String cIp) {
        this.cIp = cIp;
    }

    public String getsIp() {
        return sIp;
    }

    public void setsIp(String sIp) {
        this.sIp = sIp;
    }

    public String getCsUriStem() {
        return csUriStem;
    }

    public void setCsUriStem(String csUriStem) {
        this.csUriStem = csUriStem;
    }

    public String getCsUserAgent() {
        return csUserAgent;
    }

    public void setCsUserAgent(String csUserAgent) {
        this.csUserAgent = csUserAgent;
    }

//	@Override
//	public String toString() {
//		return "SDCLog [date=" + date + ", time=" + time + "]";
//	}


    @Override
    public String toString() {
        String column = date + "|" + time + "|" + cIp + "|"+csUserAgent+"|";

        String query = getUriQuery().get(SDCLogConstants.LOG_QUERY_NAME_UTMSOURCE)
                + "|" + getUriQuery().get(SDCLogConstants.LOG_QUERY_NAME_WTUTYPE)
                +"|"+getUriQuery().get(SDCLogConstants.LOG_QUERY_NAME_WTAVV)
                +"|"+getUriQuery().get(SDCLogConstants.LOG_QUERY_NAME_WTPOS)+"|";

        String event = getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTLOGIN)
                +"|"+getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTMENU)
                +"|"+getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTCART)
                +"|"+getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTUSER)
                +"|"+getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTSUC)
                +"|"+getUriQuery().get(SDCLogConstants.LOG_EVENT_NAME_WTPAY);
        return column+query+event;
    }
}
