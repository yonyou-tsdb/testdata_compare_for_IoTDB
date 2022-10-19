package com.yonyou.iotdb.test;

import org.apache.iotdb.tsfile.utils.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    private final Pattern HEADER_P = Pattern.compile("(.+):(\\d+):(\\d+):(true|false)");

    public LogSummaryDataReader(File logFile) throws Exception {
        this.reader = new BufferedReader(new FileReader(logFile));
        String header = this.reader.readLine();
        Matcher matcher = HEADER_P.matcher(header);
        if(matcher.find()) {
            System.out.println("source:" + matcher.group(1) + ":" + matcher.group(2));
            this.endTimestamp = Long.valueOf(matcher.group(3));
            this.saveDataIntegrity = Boolean.valueOf(matcher.group(4));
        }
        this.version = this.reader.readLine();
        String sgStr = this.reader.readLine();
        if(sgStr == null || !sgStr.startsWith(TestCompareSnapshootLog.SGLIST_MARK)) {
            throw new Exception("Error occurred while parsing sgList");
        }
        String[] sgs = sgStr.replaceFirst(TestCompareSnapshootLog.SGLIST_MARK, "").split(",");
        sgList = Arrays.stream(sgs).collect(Collectors.toList());
    }

    @Override
    public long readEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public boolean readSaveDataIntegrity() {
        return saveDataIntegrity;
    }

    @Override
    public String readVersion() {
        return version;
    }

    @Override
    public List<String> readSgList() {
        return sgList;
    }

    @Override
    public List<Pair<String, String>> readDevcieList(String sg) throws Exception {
        String sgStart = this.reader.readLine();
        if(!(TestCompareSnapshootLog.STARTSG_MARK+sg).trim().equalsIgnoreCase(sgStart.trim())) {
            throw new Exception("Error occurred while parsing sgStart");
        }
        String deviceStr = this.reader.readLine();
        if(deviceStr == null || !deviceStr.startsWith(TestCompareSnapshootLog.DEVICELIST_MARK)) {
            throw new Exception("Error occurred while parsing sgList");
        }
        String[] ds = deviceStr.replaceFirst(TestCompareSnapshootLog.DEVICELIST_MARK, "").split(",");
        List<String> tempds = Arrays.stream(ds).collect(Collectors.toList());
        List<Pair<String, String>> result = new ArrayList<>(tempds.size());
        tempds.forEach(d->result.add(new Pair<>(d, "false")));
        return result;
    }

    @Override
    public SortedMap<String, String> readMMap(String device) throws Exception {

        String deviceStart = this.reader.readLine();
        if(!(TestCompareSnapshootLog.STARTDEVICE_MARK+device).trim().equalsIgnoreCase(deviceStart.trim())) {
            throw new Exception("Error occurred while parsing deviceStart");
        }
        SortedMap<String, String> mmap = new TreeMap<>();
        String line = null;
        while((line = this.reader.readLine()) != null && !line.equalsIgnoreCase(TestCompareSnapshootLog.ENDMMAP_MARK)) {
            String measurement = line.substring(0, line.indexOf("->"));
            mmap.put(measurement, line.substring(line.indexOf("->")+2));
        }
        return mmap;
    }

    @Override
    public String readCount(String device, long endTimestamp) throws Exception {
        return readString();
    }

    @Override
    public String readMinMaxTime(String device, long endTimestamp) throws Exception {
        return readString();
    }

    @Override
    public String readCount(String mark, String device, long beginTime, long endTimestamp) throws Exception {
        return readString();
    }

    @Override
    public String readLimitTop(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception {
        return readString();
    }

    @Override
    public String readLimitBottom(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception {
        return readString();
    }

    @Override
    public String readTop(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception {
        return readString();
    }

    @Override
    public String readBottom(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception {
        return readString();
    }

    public String readString() throws Exception {
        String line;
        StringBuilder sb = new StringBuilder();
        while((line = this.reader.readLine()) != null && !line.equalsIgnoreCase(TestCompareSnapshootLog.ENDLINE_MARK)) {
            sb.append(line);
            sb.append(System.lineSeparator());
        }
        return sb.toString();
    }

    @Override
    public void close() throws Exception {
        if(this.reader != null) {
            this.reader.close();
        }
    }
}
