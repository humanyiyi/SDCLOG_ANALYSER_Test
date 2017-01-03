package com.udbac.hadoop.entity;

import java.util.Map;

/**
 * Created by root on 2016/12/30.
 */
public class SDCLog {
    private String date_time;
    private String c_ip;
    private String cs_username;
    private String cs_host;
    private String cs_method;
    private String cs_uri_stem;
    private Map<String,String> cs_uri_query;
    private String sc_status;
    private String sc_bytes;
    private String cs_version;
    private String cs_user_agent;
    private String cs_cookie;
    private String cs_referer;
    private String dcs_id;

    public String getDate_time() {
        return date_time;
    }

    public void setDate_time(String date_time) {
        this.date_time = date_time;
    }

    public String getC_ip() {
        return c_ip;
    }

    public void setC_ip(String c_ip) {
        this.c_ip = c_ip;
    }

    public String getCs_username() {
        return cs_username;
    }

    public void setCs_username(String cs_username) {
        this.cs_username = cs_username;
    }

    public String getCs_host() {
        return cs_host;
    }

    public void setCs_host(String cs_host) {
        this.cs_host = cs_host;
    }

    public String getCs_method() {
        return cs_method;
    }

    public void setCs_method(String cs_method) {
        this.cs_method = cs_method;
    }

    public String getCs_uri_stem() {
        return cs_uri_stem;
    }

    public void setCs_uri_stem(String cs_uri_stem) {
        this.cs_uri_stem = cs_uri_stem;
    }

    public Map<String, String> getCs_uri_query() {
        return cs_uri_query;
    }

    public void setCs_uri_query(Map<String, String> cs_uri_query) {
        this.cs_uri_query = cs_uri_query;
    }

    public String getSc_status() {
        return sc_status;
    }

    public void setSc_status(String sc_status) {
        this.sc_status = sc_status;
    }

    public String getSc_bytes() {
        return sc_bytes;
    }

    public void setSc_bytes(String sc_bytes) {
        this.sc_bytes = sc_bytes;
    }

    public String getCs_version() {
        return cs_version;
    }

    public void setCs_version(String cs_version) {
        this.cs_version = cs_version;
    }

    public String getCs_user_agent() {
        return cs_user_agent;
    }

    public void setCs_user_agent(String cs_user_agent) {
        this.cs_user_agent = cs_user_agent;
    }

    public String getCs_cookie() {
        return cs_cookie;
    }

    public void setCs_cookie(String cs_cookie) {
        this.cs_cookie = cs_cookie;
    }

    public String getCs_referer() {
        return cs_referer;
    }

    public void setCs_referer(String cs_referer) {
        this.cs_referer = cs_referer;
    }

    public String getDcs_id() {
        return dcs_id;
    }

    public void setDcs_id(String dcs_id) {
        this.dcs_id = dcs_id;
    }
}
