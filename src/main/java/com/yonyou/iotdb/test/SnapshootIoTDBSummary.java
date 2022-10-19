package com.yonyou.iotdb.test;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        Pattern compile = Pattern.compile(REX_SOURCE);
        Matcher matcher = compile.matcher(source);
        if(!matcher.find()) {
            throw new Exception("参数错误，1:源数据库ip:port@@username@@password，" + source);
        }
        String ip = matcher.group(1);
        String port = matcher.group(2);
        String username = matcher.group(3);
        String password = matcher.group(4);
        Session session = new Session(ip, port, username, password);
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
        session.open(false);
        session.setFetchSize(100);

        try {
            // log start
            log.writeHeader(ip, port, endTimestamp, saveDataIntegrity);
            // -----schema export-------
            SessionDataSet versionSet = session.executeQueryStatement("show version");
            SessionDataSet.DataIterator vit = versionSet.iterator();
            vit.next();
            String version = vit.getString("        version");
            if(version == null) {
                throw new Exception("iotdb版本为空");
            }
            Version curVersion;
            if(version.startsWith("0.13")) {
                curVersion = Version.V_0_13;
            } else if (version.startsWith("0.12")) {
                curVersion = Version.V_0_12;
            } else {
                throw new Exception("仅支持iotdb v0.12 v0.13");
            }
            log.writeVersion(curVersion.toString());
            session.setVersion(curVersion);
            session.setQueryTimeout(60000);
            // ~storage group export
            SessionDataSet storageGroupsSet = session.executeQueryStatement("show storage group");
            SessionDataSet.DataIterator it = storageGroupsSet.iterator();
            List<String> sgList = new ArrayList<>(2);
            while(it.next()) {
                String sg = it.getString("storage group");
                sgList.add(sg);
            }
            Collections.sort(sgList);
            System.out.println(sgList);
            log.writeSgList(sgList);
            // ~device export
            for (String sg : sgList) {
                log.writeStartSg(sg);
                SessionDataSet deviceSet = session.executeQueryStatement("show devices " + sg + ".*");
                SessionDataSet.DataIterator dit = deviceSet.iterator();
                List<Pair<String, String>> deviceList = new ArrayList<>(64);
                while (dit.next()) {
                    Pair<String, String> device = new Pair<>(dit.getString("devices"), curVersion == Version.V_0_12 ? "false" : dit.getString("isAligned"));
                    deviceList.add(device);
                }
                deviceList.sort((o1, o2) -> {
                    if (o1 == null || o1.left == null) {
                        return -1;
                    }
                    if (o2 == null || o2.left == null) {
                        return 1;
                    }
                    return o1.left.compareTo(o2.left);
                });
                System.out.println(deviceList);
                log.writeDeviceList(deviceList);
                // ~timeseries export
                for (Pair<String, String> stringStringPair : deviceList) {
                    String curDevice = stringStringPair.left;
                    log.writeStartDevice(curDevice);
                    SessionDataSet measurementSet = session.executeQueryStatement("show timeseries " + curDevice + ".*");
                    SessionDataSet.DataIterator mit = measurementSet.iterator();
                    SortedMap<String, String> mmap = new TreeMap<>();
                    while (mit.next()) {
                        String measurement = mit.getString("timeseries");
                        String timeseriesStr = measurement + "," + mit.getString("alias") + "," + mit.getString("dataType") + "," + mit.getString("encoding") + "," + mit.getString("compression") + "," + mit.getString("attributes") + "," + mit.getString("tags");
                        mmap.put(measurement, timeseriesStr);
                        System.out.println(timeseriesStr);
                    }
                    log.writeMMap(mmap);
                    // -----data count export-------
                    String countString = getResultString("count(*) " + curDevice, session, "select count(*) from " + curDevice + " where time<=" + endTimestamp);
                    System.out.println(countString);
                    log.write(countString);
                    // maxTime, minTime
                    String minMaxTimeString = getResultString("minMaxTime(*) " + curDevice, session, "select min_time(*),max_time(*) from " + curDevice + " where time<=" + endTimestamp);
                    System.out.println(minMaxTimeString);
                    log.write(minMaxTimeString);
                    // -----data detail export-------
                    long curEndTime = endTimestamp;
                    String countSql = "select count(*) from %s where time > %d and time <= %d";
                    String limitTopSql = "select * from %s where time > %d and time <= %d order by time limit 100";
                    String limitBottomSql = "select * from %s where time > %d and time <= %d order by time desc limit 100";
                    String topSql = "select top_k(*,'k'='100') from %s where time > %d and time <= %d";
                    String bottomSql = "select bottom_k(*,'k'='100') from %s where time > %d and time <= %d";
//                    log.write("day data----------");
                    // nearly a month of daily data, like count,top100,bottom100,minValue,maxValue
                    for (int k = 0; k < 30; k++) {
                        long beginTime = curEndTime - DAY_AGO;
                        String countd = getResultString("countd" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(countSql, curDevice, beginTime, curEndTime));
                        System.out.println(countd);
                        log.write(countd);
                        String limitTopd = getResultString("limitTopd" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(limitTopSql, curDevice, beginTime, curEndTime));
                        System.out.println(limitTopd);
                        log.write(limitTopd);
                        String limitBottomd = getResultString("limitBottomd" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(limitBottomSql, curDevice, beginTime, curEndTime));
                        System.out.println(limitBottomd);
                        log.write(limitBottomd);
                        String topd = getResultString("topd" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(topSql, curDevice, beginTime, curEndTime));
                        System.out.println(topd);
                        log.write(topd);
                        String bottomd = getResultString("bottomd" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(bottomSql, curDevice, beginTime, curEndTime));
                        System.out.println(bottomd);
                        log.write(bottomd);
                        curEndTime = beginTime;
                    }
//                    log.write("month data----------");
                    // one year to the last month data,like count,top100,bottom100,minValue,maxValue
                    for (int k = 0; k < 11; k++) {
                        long beginTime = curEndTime - MONTH_AGO;
                        String countm = getResultString("countm" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(countSql, curDevice, beginTime, curEndTime));
                        System.out.println(countm);
                        log.write(countm);
                        String limitTopm = getResultString("limitTopm" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(limitTopSql, curDevice, beginTime, curEndTime));
                        System.out.println(limitTopm);
                        log.write(limitTopm);
                        String limitBottomm = getResultString("limitBottomm" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(limitBottomSql, curDevice, beginTime, curEndTime));
                        System.out.println(limitBottomm);
                        log.write(limitBottomm);
                        String topm = getResultString("topm" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(topSql, curDevice, beginTime, curEndTime));
                        System.out.println(topm);
                        log.write(topm);
                        String bottomm = getResultString("bottomm" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(bottomSql, curDevice, beginTime, curEndTime));
                        System.out.println(bottomm);
                        log.write(bottomm);
                        curEndTime = beginTime;
                    }
//                    log.write("year data----------");
                    // data from a year ago cout,like count,top100,bottom100,minValue,maxValue
                    long beginTime = -1;
                    String county = getResultString("county" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(countSql, curDevice, beginTime, curEndTime));
                    System.out.println(county);
                    log.write(county);
                    String limitTopy = getResultString("limitTopy" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(limitTopSql, curDevice, beginTime, curEndTime));
                    System.out.println(limitTopy);
                    log.write(limitTopy);
                    String limitBottomy = getResultString("limitBottomy" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(limitBottomSql, curDevice, beginTime, curEndTime));
                    System.out.println(limitBottomy);
                    log.write(limitBottomy);
                    String topy = getResultString("topy" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(topSql, curDevice, beginTime, curEndTime));
                    System.out.println(topy);
                    log.write(topy);
                    String bottomy = getResultString("bottomy" + beginTime + "->" + curEndTime + "(*) " + curDevice, session, String.format(bottomSql, curDevice, beginTime, curEndTime));
                    System.out.println(bottomy);
                    log.write(bottomy);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            session.close();
            log.close();
        }
    }

    private static String getResultString(String topStr, Session session, String sql) throws Exception {
        SessionDataSet set = session.executeQueryStatement(sql);
        List<String> columnNames = set.getColumnNames();
        List<String> columnTypes = set.getColumnTypes();
        SessionDataSet.DataIterator it = set.iterator();
        StringBuilder sb = new StringBuilder(topStr);
        sb.append(System.lineSeparator());
        while(it.next()) {
            for (int k = 0; k < columnNames.size(); k++) {
                String cname = columnNames.get(k);
                sb.append(cname);
                sb.append("=");
                String typeName = columnTypes.get(k);
                sb.append(getValue(typeName, cname, it));
                sb.append(",");
            }
            if(columnNames.size() != 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
        }
        return sb.toString();
    }

    private static String getValue(String type, String columnName, SessionDataSet.DataIterator it) throws Exception {
        DataType dataType = DataType.getDataType(type);
        if(dataType == null) {
            return null;
        }
        String value = null;
        switch (dataType) {
            case INT:
                value = Integer.toString(it.getInt(columnName));
                break;
            case LONG:
                value = Long.toString(it.getLong(columnName));
                break;
            case FLOAT:
                value = Float.toString(it.getFloat(columnName));
                break;
            case DOUBLE:
                value = Double.toString(it.getDouble(columnName));
                break;
            case OBJECT:
                value = it.getObject(columnName) == null ? null : it.getObject(columnName).toString();
                break;
            case STRING:
                value = it.getString(columnName);
                break;
            case BOOLEAN:
                value = Boolean.toString(it.getBoolean(columnName));
                break;
            case TIMESTAMP:
                value = it.getTimestamp(columnName) == null ? null : it.getTimestamp(columnName).toString();
            default:
                break;
        }
        return value;
    }

    enum DataType {
        STRING, INT, BOOLEAN, FLOAT, DOUBLE, LONG, OBJECT, TIMESTAMP;

        public static DataType getDataType(String type) {
            if("string".equalsIgnoreCase(type)) {
                return STRING;
            } else if("int32".equalsIgnoreCase(type)) {
                return INT;
            } else if("boolean".equalsIgnoreCase(type)) {
                return STRING;
            } else if("float".equalsIgnoreCase(type)) {
                return FLOAT;
            } else if("double".equalsIgnoreCase(type)) {
                return DOUBLE;
            } else if("int64".equalsIgnoreCase(type)) {
                return LONG;
            } else if("object".equalsIgnoreCase(type)) {
                return OBJECT;
            } else if("timestamp".equalsIgnoreCase(type)) {
                return TIMESTAMP;
            } else {
                return null;
            }
        }
    }
}
