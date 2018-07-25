package com.jum.utils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class DBBaseUtils {
    private static final Logger logger = LoggerFactory.getLogger(DBBaseUtils.class);

    public DBBaseUtils() {
    }

    public static List<String> getInsertSqlsWithCsv(String csvPath, String tableName, List<String> excludeColumn) {
        List<String> sqls = new ArrayList();
        List<DataMap> ldm = CsvUtils.getFromCsv(csvPath);
        if (CollectionUtils.isEmpty(ldm)) {
            logger.error("Csv file data is null or size = 0.");
            return sqls;
        } else {
            List<ColumnInfo> insertci = getColumnInfoByExclude(tableName, excludeColumn);
            List<String> insertSqls = new ArrayList();
            Iterator var7 = ldm.iterator();

            while(var7.hasNext()) {
                DataMap dm = (DataMap)var7.next();
                insertSqls.add(getInsertSqlByDM(tableName, insertci, dm));
            }

            sqls.addAll(insertSqls);
            return sqls;
        }
    }

    public static List<Integer> executeBatchSqls(String tableName, List<String> sqls) {
        if (CollectionUtils.isEmpty(sqls)) {
            return new ArrayList();
        } else {
            List<String> tableNames = new ArrayList();

            for(int i = 0; i < sqls.size(); ++i) {
                tableNames.add(tableName);
            }

            return executeBatchSqls((List)tableNames, sqls);
        }
    }

    public static List<Integer> executeBatchSqls(List<String> tableName, List<String> sqls) {
        List<Integer> result = new ArrayList();
        if (!isSizeEquals(tableName, sqls)) {
            logger.error("传入tableName及sql两个list大小不相等");
            return result;
        } else {
            for(int i = 0; i < sqls.size(); ++i) {
                int succ = getUpdateResultMap((String)sqls.get(i), (String)tableName.get(i));
                if (succ < 0) {
                    logger.error("SQL=" + (String)sqls.get(i) + ",tableName=" + (String)tableName.get(i) + "执行失败");
                    logger.info("SQL=" + (String)sqls.get(i) + ",tableName=" + (String)tableName.get(i) + "执行失败");
                    result.add(succ);
                    return result;
                }

                result.add(succ);
            }

            return result;
        }
    }

    public static int getUpdateResultMap(String sql, String tableName) {
        int n = 0;
        Connection dbc = null;
        ResultSet resultSet = null;
        PreparedStatement myPreparedStatement = null;
        try {
            dbc = DBHelper.druidDataSource.getConnection();
            myPreparedStatement = dbc.prepareStatement(sql);
            n = myPreparedStatement.executeUpdate();
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("数据库操作出错", e);
            }
            return -1;
        } finally {
            closeDB(dbc, resultSet, myPreparedStatement);
        }
        return n;
    }

    public static List<ColumnInfo> getColumnInfoByExclude(String tableName, List<String> excludeColumn) {
        List<ColumnInfo> mainCol = new ArrayList();
        if (StringUtils.isBlank(tableName)) {
            logger.error("getColumnInfoByExclude params tableName " + tableName + ", List<String> is empty!");
            return mainCol;
        } else {
            List<ColumnInfo> mainColumn = getTableColumnInfo(tableName);
            mainCol.addAll(mainColumn);
            if (!CollectionUtils.isEmpty(excludeColumn)) {
                for(int i = 0; i < excludeColumn.size(); ++i) {
                    for(int j = 0; j < mainCol.size(); ++j) {
                        if (((ColumnInfo)mainCol.get(j)).getName().equalsIgnoreCase((String)excludeColumn.get(i))) {
                            mainCol.remove(j);
                        }
                    }
                }
            }

            return mainCol;
        }
    }

    public static List<ColumnInfo> getTableColumnInfo(String tableName) {
        return constructColumnInfo(getTableConfig(tableName));
    }

    private static List<ColumnInfo> constructColumnInfo(ResultSetMetaData rsd) {
        List<ColumnInfo> columnList = new ArrayList();
        if (rsd == null) {
            return columnList;
        } else {
            try {
                for(int i = 0; i < rsd.getColumnCount(); ++i) {
                    ColumnInfo column = new ColumnInfo();
                    column.setName(rsd.getColumnName(i + 1));
                    column.setSize(rsd.getColumnDisplaySize(i + 1));
                    column.setDbType(rsd.getColumnTypeName(i + 1));
                    column.setJavaType(rsd.getColumnClassName(i + 1));
                    columnList.add(column);
                }

                return columnList;
            } catch (SQLException var4) {
                logger.error("constructColumnInfo throws exception!", var4);
                return columnList;
            }
        }
    }

    private static ResultSetMetaData getTableConfig(String tableName) {
        Connection dbc = null;
        ResultSet pst = null;
        PreparedStatement myPreparedStatement = null;

        ResultSetMetaData var6;
        try {
            dbc = DBHelper.druidDataSource.getConnection();
            String sql = "select * from " + tableName + " limit 10";
            myPreparedStatement = dbc.prepareStatement(sql);
            pst = myPreparedStatement.executeQuery(sql);
            ResultSetMetaData rsmd = pst.getMetaData();
            var6 = rsmd;
        } catch (SQLException var10) {
            logger.error("getTableConfig throws exception!", var10);
            throw new RuntimeException(var10);
        } finally {
            closeDB(dbc, pst, myPreparedStatement);
        }

        return var6;
    }

    public static void closeDB(Connection dbc, ResultSet pst, PreparedStatement myPreparedStatement) {
        try {
            if (pst != null) {
                pst.close();
            }

            if (myPreparedStatement != null) {
                myPreparedStatement.close();
            }

            if (dbc != null) {
                dbc.close();
            }
        } catch (SQLException var4) {
            logger.error("关闭数据库链接失败", var4);
        }

    }

    private static String getInsertSqlByDM(String tableName, List<ColumnInfo> cilist, DataMap dm) {
        List<ColumnValue> lc = new ArrayList();
        Iterator var4 = cilist.iterator();

        while(var4.hasNext()) {
            ColumnInfo ci = (ColumnInfo)var4.next();
            ColumnValue cv = new ColumnValue();
            cv.setInfo(ci);
            cv.setValue(dm.getStringValue(ci.getName()));
            lc.add(cv);
        }

        return insertDataSQL(tableName, lc);
    }

    private static String getDeleteSqlByDM(String tableName, List<ColumnInfo> cilist, DataMap dm) {
        List<ColumnValue> lc = new ArrayList();
        Iterator var4 = cilist.iterator();

        while(var4.hasNext()) {
            ColumnInfo ci = (ColumnInfo)var4.next();
            ColumnValue cv = new ColumnValue();
            cv.setInfo(ci);
            cv.setValue(dm.getStringValue(ci.getName()));
            lc.add(cv);
        }

        return deleteDataSQL(tableName, lc);
    }

    public static String insertDataSQL(String tableName, List<ColumnValue> cvList) {
        if (CollectionUtils.isEmpty(cvList)) {
            logger.error("insertDataSQL params List<ColumnValue> is null or size == 0 ");
            return "";
        } else {
            String name = "";
            String value = "";

            for(Iterator var4 = cvList.iterator(); var4.hasNext(); value = value + ",") {
                ColumnValue cv = (ColumnValue)var4.next();
                ColumnInfo col = cv.getInfo();
                name = name + col.getName();
                name = name + ",";
                value = value + transformColumn(cv);
            }

            String nameStr = name.substring(0, name.length() - ",".length());
            String valueStr = value.substring(0, value.length() - ",".length());
            return "INSERT  INTO " + tableName + "(" + nameStr + ") VALUES (" + valueStr + ")";
        }
    }

    public static String deleteDataSQL(String tableName, List<ColumnValue> cvList) {
        if (!StringUtils.isBlank(tableName) && !CollectionUtils.isEmpty(cvList)) {
            String wsql = handleWhere(cvList, tableName);
            return "DELETE  FROM " + tableName + wsql;
        } else {
            logger.error("deleteDataSQL params tableInfo is null , List<ColumnValue> is null or size == 0 ");
            return "";
        }
    }

    private static String handleWhere(List<ColumnValue> cvList, String tableName) {
        if (CollectionUtils.isEmpty(cvList)) {
            return "";
        } else {
            String sql = " WHERE ";
            Iterator var3 = cvList.iterator();

            while(var3.hasNext()) {
                ColumnValue cv = (ColumnValue)var3.next();
                String value = transformColumn(cv);
                String conn = handleNullConnector(value);
                if (null != conn) {
                    sql = sql + cv.getInfo().getName() + conn + value + " AND ";
                }
            }

            return sql.substring(0, sql.length() - " AND ".length());
        }
    }

    private static String handleNullConnector(String value) {
        if (null != value) {
            return value.equalsIgnoreCase("NULL") ? " IS " : " = ";
        } else {
            return value;
        }
    }

    public static String transformColumn(ColumnValue columnValue) {
        if (columnValue != null && columnValue.getValue() != null) {
            ColumnInfo columnInfo = columnValue.getInfo();
            String value = columnValue.getValue();
            String JavaType = columnInfo.getJavaType();
            if (value.equalsIgnoreCase("now()")) {
                value = String.valueOf(System.currentTimeMillis());
            } else if (value.equalsIgnoreCase("uuid()")) {
                value = String.valueOf(UUID.randomUUID());
            }

            if ("java.lang.String".equals(JavaType)) {
                value = "'" + value + "'";
            } else if ("java.sql.Date".equals(JavaType)) {
                value = "'" + value + "'";
            } else if ("java.sql.Timestamp".equals(JavaType)) {
                value = "'" + value + "'";
            } else if (!"java.lang.Long".equals(JavaType) && "java.lang.Integer".equals(JavaType)) {
                ;
            }

            if (StringUtils.isBlank(value) || value.equals("''")) {
                value = null;
            }

            return value;
        } else {
            logger.error("columnValue is null!");
            return null;
        }
    }

    private static boolean isSizeEquals(List<String> tableName, List<String> sqls) {
        if (!CollectionUtils.isEmpty(tableName) && !CollectionUtils.isEmpty(sqls)) {
            if (tableName.size() != sqls.size()) {
                logger.error("传入tableName及sql两个list大小不相等");
                return false;
            } else {
                return true;
            }
        } else {
            logger.error("传入tableName及sql两个list为空");
            return false;
        }
    }

    public static List<String> getSqlsWithCsv(String csvPath, String tableName, List<String> excludeColumn, List<String> includeColumn) {
        return getSqlsWithDM(CsvUtils.getFromCsv(csvPath), tableName, excludeColumn, includeColumn);
    }

    public static List<String> getSqlsWithDM(List<DataMap> ldm, String tableName, List<String> excludeColumn, List<String> includeColumn) {
        List<String> sqls = new ArrayList();
        if (CollectionUtils.isEmpty(ldm)) {
            logger.error("Csv file data is null or size = 0.");
            return sqls;
        } else {
            List<ColumnInfo> deleteci = getColumnInfoByInclude(tableName, includeColumn);
            List<ColumnInfo> insertci = getColumnInfoByExclude(tableName, excludeColumn);
            List<String> deleteSqls = new ArrayList();
            List<String> insertSqls = new ArrayList();
            Iterator var9 = ldm.iterator();

            while(var9.hasNext()) {
                DataMap dm = (DataMap)var9.next();
                String oper = dm.getStringValue("DB_OPER");
                if (StringUtils.isBlank(oper)) {
                    deleteSqls.add(getDeleteSqlByDM(tableName, deleteci, dm));
                    insertSqls.add(getInsertSqlByDM(tableName, insertci, dm));
                } else if (oper.equalsIgnoreCase("A")) {
                    insertSqls.add(getInsertSqlByDM(tableName, insertci, dm));
                } else if (oper.equalsIgnoreCase("D")) {
                    deleteSqls.add(getDeleteSqlByDM(tableName, deleteci, dm));
                } else {
                    deleteSqls.add(getDeleteSqlByDM(tableName, deleteci, dm));
                    insertSqls.add(getInsertSqlByDM(tableName, insertci, dm));
                }
            }

            sqls.addAll(deleteSqls);
            sqls.addAll(insertSqls);
            return sqls;
        }
    }

    public static List<ColumnInfo> getColumnInfoByInclude(String tableName, List<String> includeColumn) {
        List<ColumnInfo> newCol = new ArrayList();
        if (!StringUtils.isBlank(tableName) && !CollectionUtils.isEmpty(includeColumn)) {
            List<ColumnInfo> colList = getTableColumnInfo(tableName);
            if (CollectionUtils.isEmpty(colList)) {
                return colList;
            } else {
                Iterator i$ = includeColumn.iterator();

                while(true) {
                    while(i$.hasNext()) {
                        String in = (String)i$.next();
                        Iterator var6 = colList.iterator();

                        while(var6.hasNext()) {
                            ColumnInfo ci = (ColumnInfo)var6.next();
                            if (ci.getName().equalsIgnoreCase(in)) {
                                newCol.add(ci);
                                break;
                            }
                        }
                    }

                    return newCol;
                }
            }
        } else {
            logger.error("SQLExecutor.getColumnInfoByInclude params tableName is " + tableName + ", List<String> is empty!");
            return newCol;
        }
    }
}
