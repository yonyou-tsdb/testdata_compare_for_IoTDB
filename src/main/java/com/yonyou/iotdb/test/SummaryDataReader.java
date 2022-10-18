package com.yonyou.iotdb.test;

import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;
import java.util.SortedMap;

public interface SummaryDataReader {

    long readEndTimestamp();

    boolean readSaveDataIntegrity();

    String readVersion();

    List<String> readSgList();

    List<Pair<String, String>> readDevcieList(String sg) throws Exception;

    SortedMap<String, String> readMMap(String device) throws Exception;

    String readCount(String device, long endTimestamp) throws Exception;

    String readMinMaxTime(String device, long endTimestamp) throws Exception;

    String readCount(String device, long beginTime, long endTimestamp) throws Exception;

    String readMinMaxValue(String device, long beginTime, long endTimestamp) throws Exception;

    String readLimitTop(String device, long beginTime, long endTimestamp, int count) throws Exception;

    String readLimitBottom(String device, long beginTime, long endTimestamp, int count) throws Exception;

    String readTop(String device, long beginTime, long endTimestamp, int count) throws Exception;

    String readBottom(String device, long beginTime, long endTimestamp, int count) throws Exception;
}
