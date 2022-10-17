package com.yonyou.iotdb.test;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

public class UpgradeTestFrom2To3 {

    public static void main(String[] args) throws Exception {

        // 参数校验
        // 1:概要快照文件/目标数据库ip:port@username@password，如果是文件则以f:开头
        // 2:目标数据库ip:port@username@password

        // new source/target session
        // open
        // set fetch size

        // -----schema compare-------
        // ~storage group compare
        // ~device compare
        // ~timeseries compare

        // -----data count compare-------

        // -----data detail compare-------


        Session session = new Session("127.0.0.1", 6663, "root", "root");
        session.open(false);

        // set session fetchSize
        session.setFetchSize(10000);

        SessionDataSet show_timeseries = session.executeQueryStatement("show timeseries");
        SessionDataSet.DataIterator iterator = show_timeseries.iterator();
        while(iterator.next()) {
            String timeseries = iterator.getString("timeseries");
            String alias = iterator.getString("alias");
            String storageGroup = iterator.getString("storage group");
            String dataType = iterator.getString("dataType");
            String encoding = iterator.getString("encoding");
            String compression = iterator.getString("compression");
            String tags = iterator.getString("tags");
            String attributes = iterator.getString("attributes");
            System.out.println(timeseries + ", " + alias + ", " + storageGroup + ", " + dataType + ", " + encoding + ", " + compression + ", " + tags + ", " + attributes);

//      Gson gson = new GsonBuilder().create();
//      Map<String, String> tagsMap = new HashMap<>();
//      Map<String, String> attrsMap = new HashMap<>();
//      if(tags != null) {
//        JsonObject jsonObject = gson.fromJson(tags, JsonObject.class);
//        jsonObject.entrySet().forEach((v) -> {
//          tagsMap.put(v.getKey(), v.getValue().getAsString());
//        });
//      }
//      if(attributes != null) {
//        JsonObject jsonObject = gson.fromJson(attributes, JsonObject.class);
//        jsonObject.entrySet().forEach((v) -> {
//          attrsMap.put(v.getKey(), v.getValue().getAsString());
//        });
//      }
        }
        session.close();
    }
}
