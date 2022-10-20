package com.yonyou.iotdb.test;

import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;

/**
 * summary data export from IoTDB
 *
 * @author pengfeiliu
 */
public class SnapshootIoTDBSummary {

    public static final String REX_SOURCE = "(.+):(\\d+)@@(.+)@@(.+)";

    public static final long MONTH_AGO = 30 * 24 * 60 * 60 * 1000L;
    public static final long DAY_AGO = 24 * 60 * 60 * 1000L;

    public static void main(String[] args) throws Exception {

        // param check
        // 1:data source ip:port@@username@@password
        // 2:target file parent path
        // 3:endTimeStamp
        // 4:whether to save unencrypted data
        if(args.length < 2) {
            throw new Exception("参数错误，1:源数据库ip:port@@username@@password\n" +
                    "           2:快照文件生成目录\n" +
                    "           3:endTimeStamp，默认当前时间\n" +
                    "           4:是否完整保存数据，默认md5保存");
        }
        // new source/target session
        // open
        // set fetch size
        String source = args[0];
        String outputDirStr = args[1];
        String endTimestampStr = null;
        String saveDataIntegrityStr = null;
        if(args.length == 3) {
            endTimestampStr = args[2];
        } else if(args.length >= 4) {
            endTimestampStr = args[2];
            saveDataIntegrityStr = args[3];
        }
        IoTDBSessionSummaryDataReader reader = UpgradeTestFrom2To3.getSummaryDataReader(source);
        File outputDir = new File(outputDirStr);
        if(!outputDir.exists()) {
            throw new Exception("参数错误，2:快照文件生成目录不存在\n，" + outputDirStr);
        }
        File logFile = new File(outputDir, "testCompare.log." + System.currentTimeMillis());
        if(logFile.exists()) {
            throw new Exception("日志文件已经存在");
        }
        TestCompareSnapshootLog log = new TestCompareSnapshootLog(logFile);
        long endTimestamp = System.currentTimeMillis();
        if(endTimestampStr != null) {
            endTimestamp = Long.parseLong(endTimestampStr);
        }
        boolean saveDataIntegrity = false;
        if(saveDataIntegrityStr != null) {
            saveDataIntegrity = Boolean.parseBoolean(saveDataIntegrityStr);
        }

        try {
            // log start
            log.writeHeader(endTimestamp, saveDataIntegrity);
            // -----schema export-------
            String version = reader.readVersion();
            if(version == null) {
                throw new Exception("iotdb版本为空");
            }
            Version curVersion;
            if(version.startsWith("V_0_13")) {
                curVersion = Version.V_0_13;
            } else if (version.startsWith("V_0_12")) {
                curVersion = Version.V_0_12;
            } else {
                throw new Exception("仅支持iotdb v0.12 v0.13");
            }
            log.writeVersion(curVersion.toString());
            // ~storage group export
            List<String> sgList = reader.readSgList();
            Collections.sort(sgList);
            System.out.println(sgList);
            log.writeSgList(sgList);
            // ~device export
            for (String sg : sgList) {
                log.writeStartSg(sg);
                List<Pair<String, String>> deviceList = reader.readDevcieList(sg);
                System.out.println(deviceList);
                log.writeDeviceList(deviceList);
                // ~timeseries export
                for (Pair<String, String> stringStringPair : deviceList) {
                    String curDevice = stringStringPair.left;
                    log.writeStartDevice(curDevice);
                    SortedMap<String, String> mmap = reader.readMMap(curDevice);
                    System.out.println(mmap);
                    log.writeMMap(mmap);
                    // -----data count export-------
                    String countString = reader.readCount(curDevice, endTimestamp);
                    System.out.println(countString);
                    log.write(countString);
                    // maxTime, minTime
                    String minMaxTimeString = reader.readMinMaxTime(curDevice, endTimestamp);
                    System.out.println(minMaxTimeString);
                    log.write(minMaxTimeString);
                    // -----data detail export-------
                    long curEndTime = endTimestamp;
                    int limit = 100;
//                    log.write("day data----------");
                    // nearly a month of daily data, like count,top100,bottom100,minValue,maxValue
                    for (int k = 0; k < 30; k++) {
                        long beginTime = curEndTime - DAY_AGO;
                        String countd = reader.readCount("countd", curDevice, beginTime, curEndTime);
                        System.out.println(countd);
                        log.write(countd);
                        String limitTopd = reader.readLimitTop("limitTopd", curDevice, beginTime, curEndTime, limit);
                        System.out.println(limitTopd);
                        log.write(limitTopd);
                        String limitBottomd = reader.readLimitBottom("limitBottomd", curDevice, beginTime, curEndTime, limit);
                        System.out.println(limitBottomd);
                        log.write(limitBottomd);
                        String topd = reader.readTop("topd", curDevice, beginTime, curEndTime, limit);
                        System.out.println(topd);
                        log.write(topd);
                        String bottomd = reader.readBottom("bottomd", curDevice, beginTime, curEndTime, limit);
                        System.out.println(bottomd);
                        log.write(bottomd);
                        curEndTime = beginTime;
                    }
//                    log.write("month data----------");
                    // one year to the last month data,like count,top100,bottom100,minValue,maxValue
                    for (int k = 0; k < 11; k++) {
                        long beginTime = curEndTime - MONTH_AGO;
                        String countm = reader.readCount("countm", curDevice, beginTime, curEndTime);
                        System.out.println(countm);
                        log.write(countm);
                        String limitTopm = reader.readLimitTop("limitTopm", curDevice, beginTime, curEndTime, limit);
                        System.out.println(limitTopm);
                        log.write(limitTopm);
                        String limitBottomm = reader.readLimitBottom("limitBottomm", curDevice, beginTime, curEndTime, limit);
                        System.out.println(limitBottomm);
                        log.write(limitBottomm);
                        String topm = reader.readTop("topm", curDevice, beginTime, curEndTime, limit);
                        System.out.println(topm);
                        log.write(topm);
                        String bottomm = reader.readBottom("bottomm", curDevice, beginTime, curEndTime, limit);
                        System.out.println(bottomm);
                        log.write(bottomm);
                        curEndTime = beginTime;
                    }
//                    log.write("year data----------");
                    // data from a year ago cout,like count,top100,bottom100,minValue,maxValue
                    long beginTime = -1;
                    String county = reader.readCount("county", curDevice, beginTime, curEndTime);
                    System.out.println(county);
                    log.write(county);
                    String limitTopy = reader.readLimitTop("limitTopy", curDevice, beginTime, curEndTime, limit);
                    System.out.println(limitTopy);
                    log.write(limitTopy);
                    String limitBottomy = reader.readLimitBottom("limitBottomy", curDevice, beginTime, curEndTime, limit);
                    System.out.println(limitBottomy);
                    log.write(limitBottomy);
                    String topy = reader.readTop("topy", curDevice, beginTime, curEndTime, limit);
                    System.out.println(topy);
                    log.write(topy);
                    String bottomy = reader.readBottom("bottomy", curDevice, beginTime, curEndTime, limit);
                    System.out.println(bottomy);
                    log.write(bottomy);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            reader.close();
            log.close();
        }
    }

}
