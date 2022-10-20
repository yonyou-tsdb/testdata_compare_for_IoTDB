package com.yonyou.iotdb.test;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.*;

public class IoTDBSessionSummaryDataReader implements SummaryDataReader {

    private final Session session;
    private Version curVersion;

    private final String countSql = "select count(*) from %s where time > %d and time <= %d";
    private final String limitTopSql = "select * from %s where time > %d and time <= %d order by time limit %d";
    private final String limitBottomSql = "select * from %s where time > %d and time <= %d order by time desc limit %d";
    private final String topSql = "select top_k(*,'k'='%d') from %s where time > %d and time <= %d";
    private final String bottomSql = "select bottom_k(*,'k'='%d') from %s where time > %d and time <= %d";

    public IoTDBSessionSummaryDataReader(Session session) throws Exception {
        this.session = session;
        session.open(false);
        session.setFetchSize(100);
        session.setQueryTimeout(60000);
        SessionDataSet versionSet = session.executeQueryStatement("show version");
        SessionDataSet.DataIterator vit = versionSet.iterator();
        vit.next();
        String version = vit.getString("        version");
        if(version == null) {
            throw new Exception("iotdb版本为空");
        }

        if(version.startsWith("0.13")) {
            curVersion = Version.V_0_13;
        } else if (version.startsWith("0.12")) {
            curVersion = Version.V_0_12;
        } else {
            throw new Exception("仅支持iotdb v0.12 v0.13");
        }
        session.setVersion(curVersion);
    }

    @Override
    public long readEndTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public boolean readSaveDataIntegrity() {
        return false;
    }

    @Override
    public String readVersion() {
        return curVersion.toString();
    }

    @Override
    public List<String> readSgList() throws Exception {
        SessionDataSet storageGroupsSet = session.executeQueryStatement("show storage group");
        SessionDataSet.DataIterator it = storageGroupsSet.iterator();
        List<String> sgList = new ArrayList<>(2);
        while(it.next()) {
            String sg = it.getString("storage group");
            sgList.add(sg);
        }
        Collections.sort(sgList);
        return sgList;
    }

    @Override
    public List<Pair<String, String>> readDevcieList(String sg) throws Exception {
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
        return deviceList;
    }

    @Override
    public SortedMap<String, String> readMMap(String device) throws Exception {
        SessionDataSet measurementSet = session.executeQueryStatement("show timeseries " + device + ".*");
        SessionDataSet.DataIterator mit = measurementSet.iterator();
        SortedMap<String, String> mmap = new TreeMap<>();
        while (mit.next()) {
            String measurement = mit.getString("timeseries");
            String timeseriesStr = measurement + "," + mit.getString("alias") + "," + mit.getString("dataType") + "," + mit.getString("encoding") + "," + mit.getString("compression") + "," + mit.getString("attributes") + "," + mit.getString("tags");
            mmap.put(measurement, timeseriesStr);
        }
        return mmap;
    }

    @Override
    public String readCount(String device, long endTimestamp) throws Exception {

        return getResultString("count(*) " + device, session, "select count(*) from " + device + " where time<=" + endTimestamp);
    }

    @Override
    public String readMinMaxTime(String device, long endTimestamp) throws Exception {
        return getResultString("minMaxTime(*) " + device, session, "select min_time(*),max_time(*) from " + device + " where time<=" + endTimestamp);
    }

    @Override
    public String readCount(String mark, String device, long beginTime, long endTimestamp) throws Exception {
        return getResultString(mark+ beginTime + "->" + endTimestamp + "(*) " + device, session, String.format(countSql, device, beginTime, endTimestamp));
    }

    @Override
    public String readLimitTop(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception {
        return getResultString(mark + beginTime + "->" + endTimestamp + "(*) " + device, session, String.format(limitTopSql, device, beginTime, endTimestamp, count));
    }

    @Override
    public String readLimitBottom(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception {
        return getResultString(mark + beginTime + "->" + endTimestamp + "(*) " + device, session, String.format(limitBottomSql, device, beginTime, endTimestamp, count));
    }

    @Override
    public String readTop(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception {
        return getResultString(mark + beginTime + "->" + endTimestamp + "(*) " + device, session, String.format(topSql, count, device, beginTime, endTimestamp));
    }

    @Override
    public String readBottom(String mark, String device, long beginTime, long endTimestamp, int count) throws Exception {
        return getResultString(mark + beginTime + "->" + endTimestamp + "(*) " + device, session, String.format(bottomSql, count, device, beginTime, endTimestamp));
    }

    @Override
    public void close() throws Exception {
        if(this.session != null) {
            this.session.close();
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
