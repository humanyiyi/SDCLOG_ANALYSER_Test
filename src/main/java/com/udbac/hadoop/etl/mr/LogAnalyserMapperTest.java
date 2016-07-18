package com.udbac.hadoop.etl.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by root on 2016/7/14.
 */
public class LogAnalyserMapperTest {
    Configuration conf = new Configuration();
    MapDriver<LongWritable, Text, NullWritable, Text> mapDriver;

    @Before
    public void setup() {
        LogAnalyserMapper logAnalyserMapper = new LogAnalyserMapper();
        mapDriver = MapDriver.newMapDriver(logAnalyserMapper);

    }

    @Test
    public void testReducer() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "2016-07-12 06:20:58 221.232.15.122 - m.4008823823.com.cn GET /kfcmwos/login utm_source=superapp&utm_medium=&utm_content=&utm_campaign=&ref=&WT.branch=kfc_mwos_A&wt.es=http%3a%2f%2fm.4008823823.com.cn%2fkfcmwos%2flogin%3futm_source%3dsuperapp%26utm_medium%3d%26utm_content%3d%26utm_campaign%3d%26ref%3d&wt.sr=320x568&wt.co_f=2567bf2e0ffda1a1c871467981812808&wt.event=orderpay&wt.&wt.tx_i=1468304403387020126&wt.pn_sku=d%e8%b6%85%e7%ba%a7%e5%a4%96%e5%b8%a6%e5%85%a8%e5%ae%b6%e6%a1%b6&wt.tx_money=124.0 200 - HTTP/1.0 Mozilla/5.0+(iPhone;+CPU+iPhone+OS+9_3_2+like+Mac+OS+X)+AppleWebKit/601.1.46+(KHTML,+like+Gecko)+Mobile/13F69+(5596300800) - http://m.4008823823.com.cn/kfcmwos/imordering.do?deviceid=8ad3fcc5d0ec701ffac69cd6c720d4c6&token=8dc85ce4-20b2-4eba-8336-80633c829fae_1&auth=0850736f5b8a2f958c64fe4d5ed8603d&type=login&channel=superapp&accesscode=brand_app&utm_source=superapp&utm_mediu=ownmedia dcsoye69k00000w4dihnwa22c_7u5p"));
        mapDriver.withOutput(NullWritable.get(),new Text());
        mapDriver.runTest();
    }

}