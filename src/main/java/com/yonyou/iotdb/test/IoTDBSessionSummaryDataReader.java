package com.yonyou.iotdb.test;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;
import java.util.SortedMap;

public class IoTDBSessionSummaryDataReader implements SummaryDataReader {

    private final Session session;

    public IoTDBSessionSummaryDataReader(Session session) {
        this.session = session;
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
}
