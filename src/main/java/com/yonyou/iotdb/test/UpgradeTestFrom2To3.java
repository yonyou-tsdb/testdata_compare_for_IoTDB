package com.yonyou.iotdb.test;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

public class UpgradeTestFrom2To3 {

    public static void main(String[] args) throws Exception {

        // 参数校验
        // 1:概要快照文件/目标数据库ip:port@username@password，如果是文件则以f:开头
        // 2:目标数据库ip:port@username@password

        // new source/target session
        // open
        // set fetch size

        // -----schema compare-------
        // ~storage group compare
        // ~device compare
        // ~timeseries compare

        // -----data count compare-------

        // -----data detail compare-------

    }
}
