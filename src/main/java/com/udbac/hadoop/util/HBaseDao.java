package com.udbac.hadoop.util;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDao {

    /**
     * 获取HBase连接
     *
     * @return
     */
    public static Connection getConnection() {
        String rootPath = System.getProperty("user.dir");
        String xmlPath = rootPath + File.separator + "conf" + File.separator
                + "hbase-site.xml";
        Configuration conf = new Configuration();
        conf.addResource(xmlPath);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 创建表,多列簇
     *
     * @param tableName
     * @param columnFamilys
     * @param connection
     * @throws IOException
     */
    public static void createHTable(String tableName, String[] columnFamilys,
                                    Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            System.out.println("Table exist");
            System.exit(0);
        } else {
            HTableDescriptor htdesc = createHTDesc(tn);
            for (int i = 0; i < columnFamilys.length; i++) {
                String columnFamily = columnFamilys[i];
                addFamily(htdesc, columnFamily, false);
            }
            admin.createTable(htdesc);
            System.out.println("Table create success");
        }
        admin.close();
    }

    /**
     * 创建表,单一列簇
     *
     * @param tableName
     * @param columnFamily
     * @param connection
     * @throws IOException
     */
    public static void createHTable(String tableName, String columnFamily,
                                    Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            System.out.println("Table exist");
            System.exit(0);
        } else {
            HTableDescriptor htdesc = createHTDesc(tn);
            addFamily(htdesc, columnFamily, false);
            admin.createTable(htdesc);
            System.out.println("Table create success");
        }
        admin.close();
    }

    /**
     * 添加单一列簇
     *
     * @param tableName
     * @param familyName
     * @param connection
     * @throws IOException
     */
    public static void addColumnFamily(String tableName, String familyName,
                                       Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tn)) {
                Collection<HColumnDescriptor> families = admin
                        .getTableDescriptor(TableName.valueOf(tableName))
                        .getFamilies();
                boolean exist = false;
                for (HColumnDescriptor hColumnDescriptor : families) {
                    if (hColumnDescriptor.getNameAsString().equals(familyName)) {
                        System.out.println("colFamily:" + familyName
                                + "already exist");
                        System.exit(0);
                        break;
                    }
                }
                if (!exist) {
                    if (admin.isTableAvailable(tn))
                        admin.disableTable(tn);
                    admin.addColumn(tn, new HColumnDescriptor(familyName));
                    admin.enableTable(tn);
                    System.out.println("colFamily add success");
                }

            } else {
                System.out.println("Table not exist");
                System.exit(0);
            }
        } catch (IOException e) {
            throw e;
        } finally {
            admin.close();
        }
    }

    /**
     * 添加多列簇
     *
     * @param tableName
     * @param familyNames
     * @param connection
     * @throws IOException
     */
    public static void addColumnFamily(String tableName, String[] familyNames,
                                       Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tn)) {
                boolean exist = false;
                Collection<HColumnDescriptor> families = admin
                        .getTableDescriptor(TableName.valueOf(tableName))
                        .getFamilies();
                for (String familyName : familyNames) {
                    for (HColumnDescriptor hColumnDescriptor : families) {
                        if (hColumnDescriptor.getNameAsString().equals(
                                familyName)) {
                            System.out.println("colFamily:" + familyName
                                    + "already exist");
                            System.exit(0);
                            break;
                        }
                    }
                }

                if (!exist) {
                    if (admin.isTableAvailable(tn))
                        admin.disableTable(tn);
                    for (String familyName : familyNames) {
                        admin.addColumn(tn, new HColumnDescriptor(familyName));
                    }
                    admin.enableTable(tn);
                }
                System.out.println("colFamily add success");
            } else {
                System.out.println("Table not exist");
                System.exit(0);
            }

        } catch (IOException e) {
            throw e;
        } finally {
            admin.close();
        }

    }

    /**
     * 根据rowid 删除指定列
     *
     * @param tableName
     * @param rowID
     * @param colName
     * @param colFamily
     * @param connection
     * @throws IOException
     */
    public void deleteColumn(String tableName, String rowID, String colName,
                             String colFamily, Connection connection) throws IOException {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(rowID.getBytes());
            del.addColumn(colFamily.getBytes(), colName.getBytes());
            table.delete(del);
        } catch (IOException e) {
            throw e;
        }
    }

    /**
     * 删除指定列
     *
     * @param tableName
     * @param rowID
     * @param connection
     * @throws IOException
     */
    public void deleteColumn(String tableName, String rowID,
                             Connection connection) throws IOException {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(rowID.getBytes());
            table.delete(del);
        } catch (IOException e) {
            throw e;
        }
    }

    /**
     * 禁用表
     *
     * @param tableName
     * @param connection
     * @throws IOException
     */
    public static void disableTable(String tableName, Connection connection)
            throws IOException {
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        try {
            if (admin.isTableAvailable(tn))
                admin.disableTable(tn);
            System.out.println("table:" + tableName + "disable success");
        } catch (IOException e) {
            throw e;
        } finally {
            admin.close();
        }
    }

    /**
     * 启用表
     *
     * @param tableName
     * @param connection
     * @throws IOException
     */
    public static void enableTable(String tableName, Connection connection)
            throws IOException {
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        try {
            if (admin.isTableAvailable(tn)) {
                System.out.println("table:" + tableName + "already enable");
            } else {
                admin.enableTable(tn);
                System.out.println("table:" + tableName + "enable success");
            }
        } catch (IOException e) {
            throw e;
        } finally {
            admin.close();
        }
    }

    /**
     * 使用scan批量查询数据
     *
     * @param tableName
     * @param colFamily
     * @param colName
     * @param connection
     * @return
     * @throws IOException
     */
    public static Map<String, String> getColumnValue(String tableName,
                                                     String colFamily, String colName, Connection connection)
            throws IOException {
        ResultScanner scanner = null;
        TableName tn = TableName.valueOf(tableName);
        Table table = null;
        try {
            table = connection.getTable(tn);
            scanner = table
                    .getScanner(colFamily.getBytes(), colName.getBytes());
            Result rowResult = scanner.next();
            Map<String, String> resultMap = new HashMap<String, String>();
            String row;
            while (rowResult != null) {
                row = new String(rowResult.getRow());
                resultMap.put(
                        row,
                        new String(rowResult.getValue(colFamily.getBytes(),
                                colName.getBytes())));
                rowResult = scanner.next();
            }
            return resultMap;
        } catch (IOException e) {
            throw e;
        } finally {
            if (scanner != null) {
                scanner.close();// 一定要关闭
            }
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * get方法获取值
     *
     * @param tableName
     * @param rowID
     * @param colName
     * @param colFamily
     * @param connection
     * @return
     * @throws IOException
     */
    public static String getValue(String tableName, String rowID,
                                  String colName, String colFamily, Connection connection)
            throws IOException {
        TableName tn = TableName.valueOf(tableName);
        Table table = null;
        try {
            table = connection.getTable(tn);
            Get get = new Get(rowID.getBytes());
            Result result = table.get(get);
            byte[] b = result
                    .getValue(colFamily.getBytes(), colName.getBytes());
            if (b == null)
                return "";
            else
                return new String(b);
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * 插入或更新值
     * @param tableName
     * @param row
     * @param family
     * @param qualifier
     * @param value
     * @param connection
     * @throws IOException
     */
    public static void insertAndUpdate(String tableName, String row,
                                       String family, String qualifier, String value, Connection connection)
            throws IOException {
        TableName tn = TableName.valueOf(tableName);
        Table table = null;
        try {
            table = connection.getTable(tn);
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            table.put(put);
            System.out.println("value add or input success");
        } catch (IOException e) {
            throw e;
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * 给表添加列簇
     *
     * @param htdesc
     * @param colName
     * @param readonly
     *            是否只读
     * @throws Exception
     */
    private static void addFamily(HTableDescriptor htdesc, String colName,
                                  final boolean readonly) {
        htdesc.addFamily(createHCDesc(colName));
        htdesc.setReadOnly(readonly);
    }

    /**
     * 创建列的描述,添加后，该列会有一个冒号的后缀，用于存储(列)族, 将来如果需要扩展，那么就在该列后加入(列)族
     *
     * @param colName
     * @return
     */
    private static HColumnDescriptor createHCDesc(String colName) {
        return new HColumnDescriptor(Bytes.toBytes(fixColName(colName)));
    }

    /**
     * 针对hbase的列的特殊情况进行处理,列的情况: course: or course:math, 就是要么带列族，要么不带列族(以冒号结尾)
     *
     * @param colName
     *            列
     * @param colFamily
     *            列族
     * @return
     */
    private static String fixColName(String colName, String colFamily) {
        if (colFamily != null && colFamily.trim().length() > 0
                && colName.endsWith(colFamily)) {
            return colName;
        }

        if (colFamily == null || colFamily.trim().length() == 0) {
            return colName;
        }
        String tmp = colName;
        int index = colName.indexOf(":");
        // int leng = colName.length();
        if (index == -1) {
            tmp += ":";
        }
        // 直接加入列族
        if (colFamily != null && colFamily.trim().length() > 0) {
            tmp += colFamily;
        }
        return tmp;
    }

    private static String fixColName(String colName) {

        return fixColName(colName, null);
    }

    /**
     * 创建表的描述
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    private static HTableDescriptor createHTDesc(final TableName tableName) {
        return new HTableDescriptor(tableName);
    }

    // 删除表
    public static boolean dropTable(String tableName, Connection connection)
            throws IOException {
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        boolean flag = false;
        if (admin.tableExists(tn)) {
            try {
                admin.disableTable(tn);
                admin.deleteTable(tn);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("drop table " + tableName + " faile");
                flag = false;
            }
            System.out.println("drop table " + tableName + " success");
            flag = true;
        } else {
            System.out.println("table is not exists");
            flag = false;
        }
        admin.close();
        return flag;
    }

}