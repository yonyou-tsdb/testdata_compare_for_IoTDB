package com.yonyou.iotdb.test;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        SummaryDataReader reader = null;
        if(source.startsWith("f:")) {
            File file = new File(source.replace("f:", ""));
            if(!file.exists()) {
                throw new Exception(source + " not exists");
            }
            reader = new LogSummaryDataReader(file);
        } else {
            reader = getSummaryDataReader(source);
        }
        String target = args[1];
        IoTDBSessionSummaryDataReader targetReader = getSummaryDataReader(target);

        try {
            
        } catch (Exception e) {

        } finally {
            if(reader != null) {
                reader.close();
            }
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

    private static IoTDBSessionSummaryDataReader getSummaryDataReader(String source) throws Exception {
        IoTDBSessionSummaryDataReader reader;
        Pattern compile = Pattern.compile(SnapshootIoTDBSummary.REX_SOURCE);
        Matcher matcher = compile.matcher(source);
        if(!matcher.find()) {
            throw new Exception("参数错误，数据库ip:port@@username@@password，" + source);
        }
        String ip = matcher.group(1);
        String port = matcher.group(2);
        String username = matcher.group(3);
        String password = matcher.group(4);
        Session session = new Session(ip, port, username, password);
        reader = new IoTDBSessionSummaryDataReader(session);
        return reader;
    }
}
