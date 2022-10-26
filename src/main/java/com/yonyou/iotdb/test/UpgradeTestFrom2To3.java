package com.yonyou.iotdb.test;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.yonyou.iotdb.test.SnapshootIoTDBSummary.DAY_AGO;
import static com.yonyou.iotdb.test.SnapshootIoTDBSummary.MONTH_AGO;

/**
 * summary data compare with IoTDB
 *
 * @author pengfeiliu
 */
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
        long compareBeginTime = System.currentTimeMillis();
        boolean compareSuccess = false;
        try {
            long endTimestamp = reader.readEndTimestamp();
            boolean saveDataIntegrity = reader.readSaveDataIntegrity();
            List<String> sgList = reader.readSgList();
            List<String> sgListTarget = targetReader.readSgList();
            if(sgList == null || sgListTarget == null || sgList.size() != sgListTarget.size()) {
                throw new Exception("sgList compare fail");
            }
            for (int i = 0; i < sgList.size(); i++) {
                String sg = sgList.get(i);
                String sgTargt = sgListTarget.get(i);
                if(!Objects.equals(sg, sgTargt)) {
                    throw new Exception("sg " + i + " compare fail");
                }
                List<Pair<String, String>> deviceList = reader.readDevcieList(sg);
                List<Pair<String, String>> deviceListTarget = targetReader.readDevcieList(sg);
                if(deviceList == null && deviceListTarget == null) {
                    continue;
                }
                if(deviceList == null || deviceListTarget == null || deviceList.size() != deviceListTarget.size()) {
                    throw new Exception("deviceList compare fail");
                }
                for (int j = 0; j < deviceList.size(); j++) {
                    Pair<String, String> device = deviceList.get(j);
                    Pair<String, String> deviceTargt = deviceListTarget.get(j);
                    if(device == null || deviceTargt == null || device.left == null || deviceTargt.left == null) {
                        throw new Exception("device " + j + " compare fail");
                    }
                    if(!Objects.equals(device.left, deviceTargt.left)) {
                        throw new Exception("device " + j + " compare fail");
                    }
                    SortedMap<String, String> mmap = reader.readMMap(device.left);
                    SortedMap<String, String> mmapTarget = targetReader.readMMap(deviceTargt.left);
                    if(mmap == null && mmapTarget == null) {
                        continue;
                    }
                    if(mmap == null || mmapTarget == null || mmap.size() != mmapTarget.size()) {
                        throw new Exception("device " + j + " mmap compare fail");
                    }
                    Iterator<String> it = mmap.keySet().iterator();
                    Iterator<String> itTarget = mmapTarget.keySet().iterator();
                    while(it.hasNext() && itTarget.hasNext()) {
                        String key = it.next();
                        String keyTarget = itTarget.next();
                        if(!Objects.equals(key, keyTarget)) {
                            throw new Exception("device " + j + " mmap measurement compare fail, "+key+" ≠" + keyTarget);
                        }
                        String mv = mmap.get(key);
                        String mvTarget = mmapTarget.get(keyTarget);
                        if(!Objects.equals(mv, mvTarget)) {
                            throw new Exception("device " + j + " mmap measurement compare fail, "+mv+" ≠" + mvTarget);
                        }
                    }
                    String countStr = reader.readCount(device.left, endTimestamp);
                    String countStrTarget = targetReader.readCount(deviceTargt.left, endTimestamp);
                    if(!Objects.equals(countStr.trim(), countStrTarget.trim())) {
                        throw new Exception("device " + j + " count compare fail, "+countStr+" ≠" + countStrTarget);
                    }
                    String readMinMaxStr = reader.readMinMaxTime(device.left, endTimestamp);
                    String readMinMaxStrTarget = targetReader.readMinMaxTime(deviceTargt.left, endTimestamp);
                    if(!Objects.equals(readMinMaxStr.trim(), readMinMaxStrTarget.trim())) {
                        throw new Exception("device " + j + " readMinMax compare fail, "+readMinMaxStr+" ≠" + readMinMaxStrTarget);
                    }
                    final int limit = 100;
                    long curEndTime = endTimestamp;
                    for (int k = 0; k < 30; k++) {
                        long beginTime = curEndTime - DAY_AGO;
                        readAndCompareData("d" ,reader, targetReader, j, device, limit, curEndTime, k, beginTime, saveDataIntegrity);
                        curEndTime = beginTime;
                    }
                    // one year to the last month data,like count,top100,bottom100,minValue,maxValue
                    for (int k = 0; k < 11; k++) {
                        long beginTime = curEndTime - MONTH_AGO;
                        readAndCompareData("m" ,reader, targetReader, j, device, limit, curEndTime, k, beginTime, saveDataIntegrity);
                        curEndTime = beginTime;
                    }
                    // data from a year ago cout,like count,top100,bottom100,minValue,maxValue
                    readAndCompareData("y" ,reader, targetReader, j, device, limit, curEndTime, 0, -1, saveDataIntegrity);
                }
            }
            compareSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            reader.close();
            targetReader.close();
        }
        System.out.println("完成对比，结果："+compareSuccess+"，耗时:" + (System.currentTimeMillis() - compareBeginTime) + "ms");

    }

    private static void readAndCompareData(String markPre, SummaryDataReader reader, IoTDBSessionSummaryDataReader targetReader, int j, Pair<String, String> device, int limit, long curEndTime, int k, long beginTime, boolean saveDataIntegrity) throws Exception {
        String count = reader.readCount("count" + markPre, device.left, beginTime, curEndTime);
        String countTarget = targetReader.readCount("count" + markPre, device.left, beginTime, curEndTime);
        if(!Objects.equals(count.trim(), countTarget.trim())) {
            throw new Exception("device " + j + "-" + k + " count compare fail, "+count+" ≠" + countTarget);
        }
        String limitTop = reader.readLimitTop("limitTop" + markPre, device.left, beginTime, curEndTime, limit);
        String limitTopTarget = targetReader.readLimitTop("limitTop" + markPre, device.left, beginTime, curEndTime, limit);
        if(!saveDataIntegrity) {
            limitTopTarget = SnapshootIoTDBSummary.encrypt2MD5(limitTopTarget);
        }
        if(!Objects.equals(limitTop.trim(), limitTopTarget.trim())) {
            throw new Exception("device " + j + "-" + k + " limitTop compare fail, "+limitTop+" ≠" + limitTopTarget);
        }
        String limitBottom = reader.readLimitBottom("limitBottom" + markPre, device.left, beginTime, curEndTime, limit);
        String limitBottomTarget = targetReader.readLimitBottom("limitBottom" + markPre, device.left, beginTime, curEndTime, limit);
        if(!saveDataIntegrity) {
            limitBottomTarget = SnapshootIoTDBSummary.encrypt2MD5(limitBottomTarget);
        }
        if(!Objects.equals(limitBottom.trim(), limitBottomTarget.trim())) {
            throw new Exception("device " + j + "-" + k + " limitBottom compare fail, "+limitBottom+" ≠" + limitBottomTarget);
        }
        String top = reader.readTop("top" + markPre, device.left, beginTime, curEndTime, limit);
        String topTarget = targetReader.readTop("top" + markPre, device.left, beginTime, curEndTime, limit);
        if(!saveDataIntegrity) {
            topTarget = SnapshootIoTDBSummary.encrypt2MD5(topTarget);
        }
        if(!Objects.equals(top.trim(), topTarget.trim())) {
            throw new Exception("device " + j + "-" + k + " top compare fail, "+top+" ≠" + topTarget);
        }
        String bottom = reader.readBottom("bottom" + markPre, device.left, beginTime, curEndTime, limit);
        String bottomTarget = targetReader.readBottom("bottom" + markPre, device.left, beginTime, curEndTime, limit);
        if(!saveDataIntegrity) {
            bottomTarget = SnapshootIoTDBSummary.encrypt2MD5(bottomTarget);
        }
        if(!Objects.equals(bottom.trim(), bottomTarget.trim())) {
            throw new Exception("device " + j + "-" + k + " bottom compare fail, "+bottom+" ≠" + bottomTarget);
        }
    }

    public static IoTDBSessionSummaryDataReader getSummaryDataReader(String source) throws Exception {
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
