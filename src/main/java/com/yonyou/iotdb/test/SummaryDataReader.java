package com.yonyou.iotdb.test;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;
import java.util.SortedMap;

public interface SummaryDataReader extends AutoCloseable {

    long readEndTimestamp();

    boolean readSaveDataIntegrity();

    String readVersion();

    List<String> readSgList() throws Exception;

    List<Pair<String, String>> readDevcieList(String sg) throws Exception;

    SortedMap<String, String> readMMap(String device) throws Exception;

    String readCount(String device, long endTimestamp) throws Exception;

    String readMinMaxTime(String device, long endTimestamp) throws Exception;

    String readCount(String mark, String device, long beginTime, long endTimestamp) throws Exception;

    String readLimitTop(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception;

    String readLimitBottom(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception;

//    String readTop(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception;
//
//    String readBottom(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception;
}
