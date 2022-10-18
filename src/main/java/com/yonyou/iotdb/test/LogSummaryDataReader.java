package com.yonyou.iotdb.test;

import org.apache.iotdb.tsfile.utils.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 统计数据reader（log）
 * 为避免数据量大而占用内存，所以不在构造函数全部读完，接口必须连续按约定顺序读
 */
public class LogSummaryDataReader implements SummaryDataReader, AutoCloseable {

    private final BufferedReader reader;

    private long endTimestamp;
    private boolean saveDataIntegrity;
    private String version;
    private List<String> sgList;

    private final Pattern HEADER_P = Pattern.compile("(.+):(\\d+):(\\d+):(true:false)");

    public LogSummaryDataReader(File logFile) throws IOException {
        this.reader = new BufferedReader(new FileReader(logFile));
        String header = this.reader.readLine();
        Matcher matcher = HEADER_P.matcher(header);
        if(matcher.find()) {
            System.out.println("source:" + matcher.group(1) + ":" + matcher.group(2));
            this.endTimestamp = Long.valueOf(matcher.group(3));
            this.saveDataIntegrity = Boolean.valueOf(matcher.group(4));
        }
        this.version = this.reader.readLine();

    }

    @Override
    public long readEndTimestamp() {
        return 0;
    }

    @Override
    public boolean readSaveDataIntegrity() {
        return false;
    }

    @Override
    public String readVersion() {
        return null;
    }

    @Override
    public List<String> readSgList() {
        return null;
    }

    @Override
    public List<Pair<String, String>> readDevcieList(String sg) {
        return null;
    }

    @Override
    public SortedMap<String, String> readMMap(String device) {
        return null;
    }

    @Override
    public String readCount(String device, long endTimestamp) {
        return null;
    }

    @Override
    public String readMinMaxTime(String device, long endTimestamp) {
        return null;
    }

    @Override
    public String readCount(String device, long beginTime, long endTimestamp) {
        return null;
    }

    @Override
    public String readMinMaxValue(String device, long beginTime, long endTimestamp) {
        return null;
    }

    @Override
    public String readLimitTop(String device, long beginTime, long endTimestamp, int count) {
        return null;
    }

    @Override
    public String readLimitBottom(String device, long beginTime, long endTimestamp, int count) {
        return null;
    }

    @Override
    public String readTop(String device, long beginTime, long endTimestamp, int count) {
        return null;
    }

    @Override
    public String readBottom(String device, long beginTime, long endTimestamp, int count) {
        return null;
    }

    @Override
    public void close() throws Exception {
        if(this.reader != null) {
            this.reader.close();
        }
    }
}
