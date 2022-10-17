package com.yonyou.iotdb.test;

import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.SortedMap;

public class TestCompareSnapshootLog implements AutoCloseable {

    private File logFile;
    private FileWriter writer;
    private static String LINE = System.lineSeparator();

    public TestCompareSnapshootLog(File logFile) throws IOException {
        this.logFile = logFile;
        this.writer = new FileWriter(logFile);
    }

    public void writeHeader(String ip, String port, long endTimestamp, boolean saveDataIntegrity) throws IOException {
        this.writer.write(ip + ":" + port + ":" + endTimestamp + ":" + saveDataIntegrity);
        this.writer.write(LINE);
    }

    public void writeVersion(String version) throws IOException {
        this.writer.write(version);
        this.writer.write(LINE);
    }

    public void writeSgList(List<String> sgList) throws IOException {
        this.writer.write("sgList----------");
        if(sgList != null) {
            sgList.forEach(sg-> {
                try {
                    this.writer.write(sg+LINE);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        this.writer.write(LINE);
    }

    public void writeStartSg(String sg) throws IOException {
        this.writer.write("startSg----------");
        this.writer.write(sg);
        this.writer.write(LINE);
    }

    public void writeDeviceList(List<Pair<String, String>> devices) throws IOException {
        this.writer.write("deviceList----------");
        if(devices != null) {
            devices.forEach(device->{
                if(device != null) {
                    try {
                        this.writer.write(device.left+LINE);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        this.writer.write(LINE);
    }

    public void writeStartDevice(String device) throws IOException {
        this.writer.write("startDevice----------");
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
        this.writer.write("endMMap----------");
    }

    public void write(String str) throws IOException {
        this.writer.write(str);
        this.writer.write(LINE);
    }

    @Override
    public void close() throws Exception {
        if(this.writer != null) {
            this.writer.close();
        }
    }
}
