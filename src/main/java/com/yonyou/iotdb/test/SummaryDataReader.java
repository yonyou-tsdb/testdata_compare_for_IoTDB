package com.yonyou.iotdb.test;

import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;
import java.util.SortedMap;

public interface SummaryDataReader {

    long readEndTimestamp();

    boolean readSaveDataIntegrity();

    String readVersion();

    List<String> readSgList();

    List<Pair<String, String>> readDevcieList(String sg);

    SortedMap<String, String> readMMap(String device);

    String readCount(String device, long endTimestamp);

    String readMinMaxTime(String device, long endTimestamp);

    String readCount(String device, long beginTime, long endTimestamp);

    String readMinMaxValue(String device, long beginTime, long endTimestamp);

    String readLimitTop(String device, long beginTime, long endTimestamp, int count);

    String readLimitBottom(String device, long beginTime, long endTimestamp, int count);

    String readTop(String device, long beginTime, long endTimestamp, int count);

    String readBottom(String device, long beginTime, long endTimestamp, int count);
}
