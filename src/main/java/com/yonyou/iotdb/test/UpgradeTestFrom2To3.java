package com.yonyou.iotdb.test;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

public class UpgradeTestFrom2To3 {

    public static void main(String[] args) throws Exception {

        // param check
        // 1:概要快照文件/目标数据库ip:port@username@password，如果是文件则以f:开头
        // 2:目标数据库ip:port@username@password
        if(args.length < 2) {
            throw new Exception("参数错误，1:概要快照文件/目标数据库ip:port@username@password，如果是文件则以f:开头\n" +
                    "           2:目标数据库ip:port@username@password");
        }
        // new source/target session
        String source = args[0];
        if(source.startsWith("f:")) {

        } else {

        }

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
