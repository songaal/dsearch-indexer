package com.danawa.fastcatx.indexer.ingester;

import com.danawa.fastcatx.indexer.Ingester;
import com.danawa.fastcatx.indexer.model.JdbcMetaData;
import org.apache.tomcat.util.http.fileupload.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;


public class MultipleJDBCIngester implements Ingester {
    private static final String LOB_BINARY = "LOB_BINARY";
    private static final String LOB_STRING = "LOB_STRING";

    private int bulkSize;
//    private Connection connection;

    private Connection mainConnection;
    private Connection subConnection;

//    private PreparedStatement pstmt;

    private PreparedStatement mainPstmt;
    private PreparedStatement subPstmt;

//    private ResultSet r;

    private ResultSet mainRs;
    private ResultSet subRs;

    private int lastQueryListCount;
//    private int currentQueryListCount;

    private int mainQueryListCount;
    private int subQueryListCount;

    private int columnCount;
    private String[] columnName;
    private String[] columnType;

    private Map<String, Object>[] dataSet;
    private Map<String, Object>[] subDataSet;

    private List<File> tmpFile;
    private ArrayList<String> sqlList;

    private ArrayList<String> mainSqlList;
    private ArrayList<String> subSqlList;

    private int bulkCount;
    private int subBulkCount;
    private int readCount;

    private boolean useBlobFile;

    private int fetchSize;
    private int maxRows;

    private String driverClassName;
    private String url;
    private String user;
    private String password;

    private String subDriverClassName;
    private String subUrl;
    private String subUser;
    private String subPassword;

    private String subSqlwhereclauseData;

    private boolean isClosed;

    private byte[] data = new byte[16 * 1024];
    private int totalCnt;

//    public MultipleJDBCIngester(String driverClassName, String url, String user, String password, int bulkSize, int fetchSize, int maxRows, boolean useBlobFile, ArrayList<String> sqlList) throws IOException {
//
//        this.driverClassName = driverClassName;
//        this.user = user;
//        this.url = url;
//        this.password = password;
//
//
//        this.bulkSize = bulkSize;
//        this.useBlobFile = useBlobFile;
//        this.fetchSize = fetchSize;
//        this.maxRows = maxRows;
//        this.sqlList = sqlList;
//
//        tmpFile = new ArrayList<>();
//        dataSet = new Map[bulkSize];
//        connection = getConnection(driverClassName, url, user, password);
//
//        //쿼리 갯수 체크용 카운트
//        lastQueryListCount = sqlList.size();
//        currentQueryListCount = 0;
//
//        //SQL 실행
//        logger.info("dataSQL total_Count : {}", sqlList.size());
//        executeQuery(currentQueryListCount);
//
//    }

    public MultipleJDBCIngester(Map<String, JdbcMetaData> jdbcMetaDataMap, int bulkSize, int fetchSize, int maxRows, boolean useBlobFile, Map<String, ArrayList<String>> sqlListMap, String subSqlwhereclauseData) throws IOException {
        this.bulkSize = bulkSize;
        this.fetchSize = fetchSize;
        this.maxRows = maxRows;
        this.useBlobFile = useBlobFile;
        if (jdbcMetaDataMap.containsKey("mainJDBC")) {
            JdbcMetaData mainJDBC = jdbcMetaDataMap.get("mainJDBC");

            this.driverClassName = mainJDBC.getDriverClassName();
            this.url = mainJDBC.getUrl();
            this.user = mainJDBC.getUser();
            this.password = mainJDBC.getPassword();

            this.mainSqlList = sqlListMap.get("mainSqlList");
        }

        if (jdbcMetaDataMap.containsKey("subJDBC")) { // 서브쿼리가 존재한다면..
            JdbcMetaData subJDBC = jdbcMetaDataMap.get("subJDBC");

            this.subDriverClassName = subJDBC.getDriverClassName();
            this.subUrl = subJDBC.getUrl();
            this.subUser = subJDBC.getUser();
            this.subPassword = subJDBC.getPassword();

            this.subSqlList = sqlListMap.get("subSqlList");

            this.subSqlwhereclauseData = subSqlwhereclauseData;
        }

        logger.info("{}", jdbcMetaDataMap);
        logger.info("{}, {}, {}, {}", bulkSize, fetchSize, maxRows, useBlobFile);
        logger.info("{}", sqlListMap);
        logger.info("{}", subSqlwhereclauseData);

        mainQueryListCount = 0;
        subQueryListCount = 0;

        //SQL 실행
        logger.info("dataSQL total_Count : {}", mainSqlList.size());

        executeQuery(mainQueryListCount, subQueryListCount);
    }

    public void executeQuery(int mainQueryListCount, int subQueryListCount) throws IOException {

        try {
            if (mainQueryListCount > 0) {
                mainConnection = getConnection(driverClassName, url, user, password);
            }

            if (mainPstmt != null) {
                mainPstmt.close();
            }

            if (subQueryListCount > 0) {
                subConnection = getConnection(subDriverClassName, subUrl, subUser, subPassword);
            }

            if (subPstmt != null) {
                subPstmt.close();
            }

            logger.info("Num-{} mainQuery Start", mainQueryListCount);

            if (fetchSize < 0) {
                //in mysql, fetch data row by row
                mainPstmt = mainConnection.prepareStatement(mainSqlList.get(mainQueryListCount), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                mainPstmt.setFetchSize(Integer.MIN_VALUE);

                subPstmt = mainConnection.prepareStatement(subSqlList.get(subQueryListCount), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                subPstmt.setFetchSize(Integer.MIN_VALUE);

            } else {
                mainPstmt = mainConnection.prepareStatement(mainSqlList.get(mainQueryListCount));
                subPstmt = mainConnection.prepareStatement(subSqlList.get(subQueryListCount));

                if (fetchSize > 0) {
                    // 인출할 행 수를 나타냅니다.
                    mainPstmt.setFetchSize(fetchSize);
                    subPstmt.setFetchSize(fetchSize);
                }
            }

            if (maxRows > 0) {
                // 최대 행 수를 나타내며, 제한이 없는 경우에는 0입니다.
                mainPstmt.setMaxRows(maxRows);
                subPstmt.setMaxRows(maxRows);
            }

            logger.info("메인색인쿼리 실행.");
            mainRs = mainPstmt.executeQuery(); // 메인색인쿼리 실행.

            //			ResultSetMetaData mainRsmd = mainRs.getMetaData();
            //
            //			int columCount = mainRsmd.getColumnCount(); // 컬럼의 갯수를 반환한다.
            //			int columIdx = 0;
            //			for (int idx = 0; idx < columCount; idx++) {
            //				String columnName =mainRsmd.getColumnName(idx);
            //				if (subSqlwhereclauseData.equals(columnName)){
            //					logger.info("columnName : {}", columnName);
            //					columIdx = idx;
            //				}
            //			}
            //
            //			mainRs.getString(columIdx);
            //
            if (subPstmt != null) {
                subPstmt.close();
            }
        } catch (Exception e) {
            closeConnection();
            throw new IOException(e);
        }
    }

//    public void executeQuery(int currentQueryListCount) throws IOException {
//
//        try {
//            if (currentQueryListCount > 0) {
//                connection = getConnection(driverClassName, url, user, password);
//            }
//
//            if (pstmt != null) {
//                pstmt.close();
//            }
//            logger.info("Num-{} QUERY Start", currentQueryListCount);
//            if (fetchSize < 0) {
//                //in mysql, fetch data row by row
//                pstmt = connection.prepareStatement(sqlList.get(currentQueryListCount), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
//                pstmt.setFetchSize(Integer.MIN_VALUE);
//            } else {
//                pstmt = connection.prepareStatement(sqlList.get(currentQueryListCount));
//                if (fetchSize > 0) {
//                    pstmt.setFetchSize(fetchSize);
//                }
//            }
//
//            if (maxRows > 0) {
//                pstmt.setMaxRows(maxRows);
//            }
//
//            r = pstmt.executeQuery();
//
//            ResultSetMetaData rsMetadata = r.getMetaData();
//            columnCount = rsMetadata.getColumnCount();
//            columnName = new String[columnCount];
//            columnType = new String[columnCount];
//
//            for (int i = 0; i < columnCount; i++) {
//                columnName[i] = rsMetadata.getColumnLabel(i + 1);
//                String typeName = rsMetadata.getColumnTypeName(i + 1);
//                columnType[i] = typeName;
//                logger.info("Column-{} [{}]:[{}]", new Object[]{i + 1, columnName[i], typeName});
//            }
//
//        } catch (Exception e) {
//            closeConnection();
//            throw new IOException(e);
//        }
//    }

    //다음 쿼리가 있는지 체크
    public boolean hasNextQuery(int currentQueryListCount, int lastQueryListCount) {

        if (currentQueryListCount == lastQueryListCount) {
            logger.info("Current : {} - Last : {} - Query End", currentQueryListCount, lastQueryListCount);

            //쿼리가 완전히 끝나면 커넥션을 포함한 sql close
            closeConnection();
            return false;
        } else {

            //남은 쿼리가 있다면 resultSet, pstmt 만 close
            closeConnection();
            return true;
        }
    }


    @Override
    public boolean hasNext() throws IOException {
        if (readCount >= bulkCount) {
//            fill();
            fill2();
            if (bulkCount == 0) {
                //다음 쿼리 실행
//                currentQueryListCount++;
//                if (hasNextQuery(currentQueryListCount, lastQueryListCount)) {
//                    logger.info("next Query Start : {}", currentQueryListCount);
//                    executeQuery(currentQueryListCount);
//                } else {
//                    return false;
//                }
                mainQueryListCount++;
                if (hasNextQuery(mainQueryListCount, lastQueryListCount)) {
                    logger.info("next Query Start : {}", mainQueryListCount);
                    executeQuery(mainQueryListCount, subQueryListCount);
                } else {
                    return false;
                }
            }
            readCount = 0;
        }
        return true;
    }

    @Override
    public Map<String, Object> next() throws IOException {
        if (readCount >= bulkCount) {
            fill2();
            if (bulkCount == 0)
                return null;
            readCount = 0;
        }
        return dataSet[readCount++];
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            logger.info("Close JDBCIngester.. Read {} docs.", totalCnt);
            deleteTmpLob();
            closeConnection();
            isClosed = true;
        }
    }

    private void closePstmt() {
        try {
            if (mainRs != null) {
                mainRs.close();
            }

            if (subRs != null) {
                subRs.close();
            }
        } catch (SQLException ignore) {
        }

        try {
            if (mainPstmt != null) {
                mainPstmt.close();
            }

            if (subPstmt != null) {
                subPstmt.close();
            }

        } catch (SQLException ignore) {
        }
    }

    private void closeConnection() {

        try {
            if (mainRs != null) {
                mainRs.close();
            }

            if (subRs != null) {
                subRs.close();
            }
        } catch (SQLException ignore) {
        }

        try {
            if (mainPstmt != null) {
                mainPstmt.close();
            }

            if (subPstmt != null) {
                subPstmt.close();
            }

        } catch (SQLException ignore) {
        }

        try {
            if (mainConnection != null && !mainConnection.isClosed()) {
                mainConnection.close();
            }

            if (subConnection != null && !subConnection.isClosed()) {
                subConnection.close();
            }
        } catch (SQLException ignore) {
        }
    }

    private void fill2() throws IOException {
        logger.info("fill2 start!!");
        bulkCount = 0;

        ArrayList<String> whereclauseList = new ArrayList<>();
        try {
            ResultSetMetaData mainRsmd = mainRs.getMetaData();

            int mainColumCount = mainRsmd.getColumnCount(); // 메인쿼리 컬럼의 갯수를 반환한다.

            Map<String, Object> mainKeyValue = new HashMap<>();
            Map<String, Object> subKeyValue = new HashMap<>();

            while (mainRs.next()) {
                for (int idx = 0; idx < mainColumCount; idx++) {
                    String mainColumnName = mainRsmd.getColumnName(idx);
                    if (subSqlwhereclauseData.equals(mainColumnName)) {
                        whereclauseList.add(String.valueOf(mainRs.getObject(idx))); // 써브쿼리 where 데이터
                    }

                    mainKeyValue.put(mainColumnName, mainRs.getObject(idx));
                }

                dataSet[bulkCount] = mainKeyValue;
                bulkCount++;

                if (bulkCount >= bulkSize) {
                    break;
                }
            }

            String whereclause = String.join(",", whereclauseList);
            subPstmt.setString(1, whereclause);

            subRs = subPstmt.executeQuery(); // 서브색인쿼리 실행.

            ResultSetMetaData subRsmd = mainRs.getMetaData();

            int subColumCount = subRsmd.getColumnCount(); // 서브쿼리 컬럼의 갯수를 반환한다.

            subBulkCount = 0;
            if (subRs.next()) {
                for (int idx = 0; idx < subColumCount; idx++) {
                    String subColumnName = subRsmd.getColumnName(idx);
                    subKeyValue.put(subColumnName, subRs.getObject(idx));
                }
                subDataSet[subBulkCount] = subKeyValue;
                subBulkCount++;
            }

            int logShowCnt = 0;
            for (Map<String, Object> mainData : dataSet) {
                if (mainData.containsKey(subSqlwhereclauseData)) {
                    String compareData = String.valueOf(mainData.get(subSqlwhereclauseData));

                    for (Map<String, Object> subData : subDataSet) {
                        String compareSubData = String.valueOf(subData.get(subSqlwhereclauseData));
                        if (compareData.equals(compareSubData)) {
                            mainData.put("productName", subData.get("productName"));
                        }
                    }
                }

                if (logShowCnt % 10 == 0) {
                    logger.info("productName : {}", mainData.get("productName"));
                }

                logShowCnt++;

            }

        } catch (Exception e) {
            logger.error("", e);
        }
    }

//    private void fill() throws IOException {
//
//        bulkCount = 0;
//        try {
//            ResultSetMetaData rsMeta = null;
//            //이전 Tmp 데이터들을 지워준다.
//            deleteTmpLob();
//
//            try {
//                rsMeta = r.getMetaData();
//            } catch (SQLException e) {
//                return;
//            }
//            while (r.next()) {
//
//                Map<String, Object> keyValueMap = new HashMap<String, Object>();
//
//                for (int i = 0; i < columnCount; i++) {
//                    int columnIdx = i + 1;
//                    int type = rsMeta.getColumnType(columnIdx);
//
//                    String str = "";
//
//                    String lobType = null;
//                    if (type == Types.BLOB || type == Types.BINARY || type == Types.LONGVARBINARY || type == Types.VARBINARY
//                            || type == Types.JAVA_OBJECT) {
//                        lobType = LOB_BINARY;
//                    } else if (type == Types.CLOB || type == Types.NCLOB || type == Types.SQLXML || type == Types.LONGVARCHAR || type == Types.LONGNVARCHAR) {
//                        lobType = LOB_STRING;
//                    }
//
//                    if (lobType == null) {
//                        str = r.getString(columnIdx);
//
//                        if (str != null) {
//                            keyValueMap.put(columnName[i], StringEscapeUtils.unescapeHtml(str));
//                        } else {
//                            // 파싱할 수 없는 자료형 이거나 정말 NULL 값인 경우
//                            keyValueMap.put(columnName[i], "");
//                        }
//                    } else {
//                        File file = null;
//
//                        if (lobType == LOB_BINARY) {
//                            // logger.debug("Column-"+columnIdx+" is BLOB!");
//                            // BLOB일 경우 스트림으로 받는다.
//                            ByteArrayOutputStream buffer = null;
//                            try {
//                                if (!useBlobFile) {
//                                    buffer = new ByteArrayOutputStream();
//                                }
//                                file = readTmpBlob(i, columnIdx, rsMeta, buffer);
//                                if (useBlobFile) {
//                                    keyValueMap.put(columnName[i], file);
//                                } else {
//                                    keyValueMap.put(columnName[i], buffer.toByteArray());
//                                }
//                            } finally {
//                                if (buffer != null) {
//                                    try {
//                                        buffer.close();
//                                    } catch (IOException ignore) {
//
//                                    }
//                                }
//                            }
//                        } else if (lobType == LOB_STRING) {
//                            StringBuilder sb = null;
//                            if (!useBlobFile) {
//                                sb = new StringBuilder();
//                            }
//                            file = readTmpClob(i, columnIdx, rsMeta, sb);
//                            if (useBlobFile) {
//                                keyValueMap.put(columnName[i], file);
//                            } else {
//                                keyValueMap.put(columnName[i], StringEscapeUtils.unescapeHtml(sb.toString()));
//                            }
//                        }
//
//                        //다음 레코드 진행시 지우도록 한다.
//                        if (file != null) {
//                            tmpFile.add(file);
//                        }
//                    }
//                }
//
//                dataSet[bulkCount] = keyValueMap;
//                bulkCount++;
//                totalCnt++;
//
//                if (bulkCount >= bulkSize) {
//                    break;
//                }
//            }
//
//        } catch (Exception e) {
//
//            logger.debug("", e);
//
//            try {
//                if (r != null) {
//                    r.close();
//                }
//            } catch (SQLException ignore) {
//            }
//
//            try {
//                if (pstmt != null) {
//                    pstmt.close();
//                }
//            } catch (SQLException ignore) {
//            }
//
//            try {
//                if (connection != null && !connection.isClosed()) {
//                    connection.close();
//                }
//            } catch (SQLException ignore) {
//            }
//
//            throw new IOException(e);
//        }
//    }

//    private File readTmpBlob(int columnInx, int columnNo, ResultSetMetaData rsMeta, OutputStream buffer) throws IOException, SQLException {
//        File file = null;
//        FileOutputStream os = null;
//        InputStream is = null;
//        try {
//            is = r.getBinaryStream(columnNo);
//            if (is != null) {
//                if (buffer == null) {
//                    file = File.createTempFile("blob." + columnNo, ".tmp");
//                    os = new FileOutputStream(file);
//                    // logger.debug("tmp file = "+f.getAbsolutePath());
//                }
//                for (int rlen = 0; (rlen = is.read(data, 0, data.length)) != -1; ) {
//                    if (buffer != null) {
//                        buffer.write(data, 0, rlen);
//                    } else {
//                        os.write(data, 0, rlen);
//                    }
//                }
//            }
//
//        } catch (IOException e) {
//            throw new IOException("Error while writing Blob field. column => " + rsMeta.getColumnName(columnNo));
//        } finally {
//            IOException ex = null;
//            if (os != null)
//                try {
//                    os.close();
//                } catch (IOException e) {
//                    ex = e;
//                }
//            if (is != null)
//                try {
//                    is.close();
//                } catch (IOException e) {
//                    ex = e;
//                }
//            if (ex != null) {
//                logger.error("Error while close LOB field and output file stream.", ex);
//            }
//        }
//        return file;
//    }
//
//    private File readTmpClob(int columnInx, int columnNo, ResultSetMetaData rsMeta, StringBuilder buffer) throws IOException, SQLException {
//        File file = null;
//        BufferedWriter os = null;
//        BufferedReader is = null;
//        try {
//            Reader reader = r.getCharacterStream(columnNo);
//            if (reader != null) {
//                //buffer is null when using File
//                if (buffer == null) {
//                    file = File.createTempFile("clob." + columnNo, ".tmp");
//                    os = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
//                }
//                is = new BufferedReader(reader);
//                for (String rline = ""; (rline = is.readLine()) != null; ) {
//                    if (buffer != null) {
//                        buffer.append(rline).append("\n");
//                    } else {
//                        os.write(rline);
//                        os.write("\n");
//                    }
//                }
//            }
//        } catch (IOException e) {
//            throw new IOException("Error while writing Clob field. column => " + rsMeta.getColumnName(columnNo));
//        } finally {
//            IOException ex = null;
//            if (os != null) {
//                try {
//                    os.close();
//                } catch (IOException e) {
//                    ex = e;
//                }
//            }
//            if (is != null) {
//                try {
//                    is.close();
//                } catch (IOException e) {
//                    ex = e;
//                }
//            }
//            if (ex != null) {
//                logger.error("Error while close clob field and output file stream.", ex);
//            }
//        }
//        return file;
//    }

    private void deleteTmpLob() {
        while (tmpFile.size() > 0) {
            File file = tmpFile.remove(tmpFile.size() - 1);
            try {
                if (file.exists()) {
                    FileUtils.forceDelete(file);
                }
            } catch (IOException e) {
                logger.debug("Can not delete file : {}", file.getAbsolutePath());
            }
        }
    }

    private Connection getConnection(String driverClassName, String url, String user, String password) throws IOException {
        Connection con = null;
        if (driverClassName != null && driverClassName.length() > 0) {
            try {
                Class.forName(driverClassName);

                Properties info = new Properties();
                info.put("user", user);
                info.put("password", password);
                info.put("connectTimeout", "300000");
                con = DriverManager.getConnection(url, info);
                con.setAutoCommit(true);
            } catch (Exception e) {
                throw new IOException(e);
            }
        } else {
            throw new IOException("JDBC driver is empty!");
        }
        return con;
    }
}
