package com.yonyou.iotdb.test;

import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.SortedMap;

public class TestCompareSnapshootLog implements AutoCloseable {

    private FileWriter writer;
    private static String LINE = System.lineSeparator();
    public static final String SGLIST_MARK = "----------sgList";
    public static final String STARTSG_MARK = "--------startSg";
    public static final String DEVICELIST_MARK = "------deviceList";
    public static final String STARTDEVICE_MARK = "------startDevice";
    public static final String ENDMMAP_MARK = "------endMMap";
    public static final String ENDLINE_MARK = "------endLine";

    public TestCompareSnapshootLog(File logFile) throws IOException {
        this.writer = new FileWriter(logFile);
    }

    public void writeHeader(long endTimestamp, boolean saveDataIntegrity) throws IOException {
        this.writer.write(endTimestamp + ":" + saveDataIntegrity);
        this.writer.write(LINE);
    }

    public void writeVersion(String version) throws IOException {
        this.writer.write(version);
        this.writer.write(LINE);
    }

    public void writeSgList(List<String> sgList) throws IOException {
        this.writer.write(SGLIST_MARK);
        if(sgList != null) {
            for (int i = 0; i < sgList.size(); i++) {
                this.writer.write(sgList.get(i));
                if(i != sgList.size() - 1) {
                    this.writer.write(",");
                }
            }
        }
        this.writer.write(LINE);
    }

    public void writeStartSg(String sg) throws IOException {
        this.writer.write(STARTSG_MARK);
        this.writer.write(sg);
        this.writer.write(LINE);
    }

    public void writeDeviceList(List<Pair<String, String>> devices) throws IOException {
        this.writer.write(DEVICELIST_MARK);
        if(devices != null) {
            for (int i = 0; i < devices.size(); i++) {
                this.writer.write(devices.get(i).left);
                if(i != devices.size() - 1) {
                    this.writer.write(",");
                }
            }
        }
        this.writer.write(LINE);
    }

    public void writeStartDevice(String device) throws IOException {
        this.writer.write(STARTDEVICE_MARK);
        this.writer.write(device);
        this.writer.write(LINE);
    }

    public void writeMMap(SortedMap<String, String> mmap) throws IOException {
        if(mmap != null) {
            mmap.forEach((k, v) -> {
                try {
                    this.writer.write(k);
                    this.writer.write("->");
                    this.writer.write(v);
                    this.writer.write(LINE);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        this.writer.write(ENDMMAP_MARK);
        this.writer.write(LINE);
    }

    public void write(String str) throws IOException {
        this.writer.write(str);
        this.writer.write(LINE);
        this.writer.write(ENDLINE_MARK);
        this.writer.write(LINE);
    }

    @Override
    public void close() throws Exception {
        if(this.writer != null) {
            this.writer.close();
        }
    }
}
