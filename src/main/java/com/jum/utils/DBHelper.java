package com.jum.utils;

/**
 * 数据库操作
 * 用到的时候链接
 * 执行后，统一close
 */

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.testng.Reporter;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DBHelper {
    private static final Logger logger = LoggerFactory.getLogger(DBHelper.class);

    public static DruidDataSource druidDataSource;
    private static ResultSet myResultSet = null;
    private static Connection myConnection = null;
    private static PreparedStatement myPreparedStatement = null;

    public static void setdruidDataSource(DruidDataSource druidDataSource) {
        DBHelper.druidDataSource = druidDataSource;
    }

    public static List<DataMap> executeQuery(String sql) {
        List<DataMap> dataList = new ArrayList<DataMap>();
        try {
            if (StringUtils.isBlank(sql)) {
                logger.warn("传入sql为空");
                return null;
            }
            initConnection();
            myPreparedStatement = myConnection.prepareStatement(sql);
            myResultSet = myPreparedStatement.executeQuery(sql);
            ResultSetMetaData rs = myResultSet.getMetaData();
            while (myResultSet.next()) {
                DataMap map = new DataMap();
                for (int i = 1; i <= rs.getColumnCount(); i++) {
                    map.put(rs.getColumnLabel(i).toUpperCase(), myResultSet.getObject(i));
                }
                dataList.add(map);
            }
            System.out.println(dataList);
        } catch (Exception ex) {
            logger.error("数据库链接异常", ex);
        } finally {
            closeDB();
        }
        return dataList;
    }

    public static int executeUpdate(String sql) {
        int n = 0;
        try {
            initConnection();
            myPreparedStatement = myConnection.prepareStatement(sql);
            n = myPreparedStatement.executeUpdate();
        } catch (Exception ex) {
            logger.error("数据库链接异常", ex);
            ex.printStackTrace();

        } finally {
            closeDB();
        }
        return n;
    }

    public static List<DataMap> query(String tableName, Map<String, String> conditions) {
        if ((StringUtils.isBlank(tableName)) || (CollectionUtils.isEmpty(conditions))) {
            logger.error("DBHelper.query params are error!");
            return new ArrayList<DataMap>();
        }
        String sql = "select * from " + tableName + " where ";
        for (Map.Entry<String, String> entry : conditions.entrySet()) {
            if ((!StringUtils.isBlank(entry.getValue()))) {
                sql = sql + entry.getKey() + "= " + "'" + entry.getValue() + "'" + " AND ";
            }

        }
        sql = sql.substring(0, sql.length() - " AND ".length());
        return executeQuery(sql);
    }

    public static boolean delete_like(String tableName, Map<String, String> conditions) {
        if ((StringUtils.isBlank(tableName) || CollectionUtils.isEmpty(conditions))) {
            logger.error("DBData.delete params tableName is " + tableName);
            return false;
        }
        String sql = "Delete from " + tableName + " where ";
        List<String> sqls = new ArrayList<String>();
        for (Map.Entry<String, String> entry : conditions.entrySet()) {
            sql = sql + entry.getKey() + " like " + "'" + entry.getValue() + "'" + " AND ";
        }
        sql = sql.substring(0, sql.length() - " AND ".length());
        sqls.add(sql);
        List<Integer> n = new ArrayList<Integer>();
        n = DBBaseUtils.executeBatchSqls(tableName, sqls);
        for (int i = 0; i < n.size(); i++) {
            if (n.get(i) == -1) {
                return false;
            }
        }
        return true;
    }

    public static boolean delete(String tableName, Map<String, String> conditions) {
        if ((StringUtils.isBlank(tableName) || CollectionUtils.isEmpty(conditions))) {
            logger.error("DBData.delete params tableName is " + tableName);
            return false;
        }
        String sql = "Delete from " + tableName + " where ";
        List<String> sqls = new ArrayList<String>();
        for (Map.Entry<String, String> entry : conditions.entrySet()) {
            sql = sql + entry.getKey() + "=" + "'" + entry.getValue() + "'" + " AND ";
        }
        sql = sql.substring(0, sql.length() - " AND ".length());
        sqls.add(sql);
        List<Integer> n = new ArrayList<Integer>();
        n = DBBaseUtils.executeBatchSqls(tableName, sqls);
        for (int i = 0; i < n.size(); i++) {
            if (n.get(i) == -1) {
                return false;
            }
        }
        return true;
    }

    public static boolean insert(String csvPath, String tableName, List<String> excludeColumn) {
        try {
            if ((StringUtils.isBlank(csvPath)) || (StringUtils.isBlank(tableName))) {
                logger.error("DBData.insert params csvPath is " + csvPath + ", tableName is " + tableName);
                return false;
            }
            List<String> sqls = DBBaseUtils.getInsertSqlsWithCsv(csvPath, tableName, excludeColumn);
            if (CollectionUtils.isEmpty(sqls)) {
                logger.error("sqls为空");
                return false;
            }
            List<Integer> n = new ArrayList<Integer>();
            n = DBBaseUtils.executeBatchSqls(tableName, sqls);
            for (int i = 0; i < n.size(); i++) {
                if (n.get(i) == -1) {
                    return false;
                }
            }
            // 从csv路径解析 类名称，作为关键字放入log中 csv路径 "classnameData_表名.csv"格式
            int beginDex = csvPath.lastIndexOf("/");
            int endDex = csvPath.lastIndexOf("Data");
            String className = csvPath.substring(beginDex + 1, endDex);
            Reporter.log("准备数据:|" + className + "|" + tableName + "|" + CsvUtils.csvToStr(csvPath), true);
            return true;
        } catch (Exception e) {
            logger.error("发生异常", e);
            return false;
        }
    }

    /**
     *
     * @param csvPath
     *            csv文件，路径
     * @param tableName
     *            表名
     * @param excludeColumn
     *            新增的时候排除指定字段
     * @param includeColumn
     *            删除的时候根据指定字段删除
     */
    public static boolean exec(String csvPath, String tableName, List<String> excludeColumn,
                               List<String> includeColumn) {
        try {

            if ((StringUtils.isBlank(csvPath)) || (StringUtils.isBlank(tableName))
                    || (CollectionUtils.isEmpty(includeColumn))) {
                logger.error("DBData.exec params are error!");
                return false;
            }
            List<String> sqls = DBBaseUtils.getSqlsWithCsv(csvPath, tableName, excludeColumn, includeColumn);

            List<Integer> n = new ArrayList<Integer>();
            n = DBBaseUtils.executeBatchSqls(tableName, sqls);
            for (int i = 0; i < n.size(); i++) {
                if (n.get(i) == -1) {
                    return false;
                }
            }

            // 从csv路径解析 类名称，作为关键字放入log中 csv路径 "classnameData_表名.csv"格式
            int beginDex = csvPath.lastIndexOf("/");
            int endDex = csvPath.lastIndexOf("Data");
            String className = csvPath.substring(beginDex + 1, endDex);
            Reporter.log("准备数据:|" + className + "|" + tableName + "|" + CsvUtils.csvToStr(csvPath), true);
            return true;
        } catch (Exception e) {
            logger.error("发生异常", e);
            return false;
        }
    }

    private static Connection initConnection() {
        try {
            myConnection = druidDataSource.getConnection();
        } catch (SQLException e) {
            logger.error("创建数据库链接失败", e);
        }
        return myConnection;
    }

    public static void closeDB() {
        try {
            if (myResultSet != null) {
                myResultSet.close();
            }
            if (myPreparedStatement != null) {
                myPreparedStatement.close();
            }
            if (myConnection != null) {
                myConnection.close();
            }
        } catch (SQLException e) {
            logger.error("关闭数据库链接失败", e);
        }
    }

}
