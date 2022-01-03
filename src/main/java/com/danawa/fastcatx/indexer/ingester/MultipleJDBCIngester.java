package com.danawa.fastcatx.indexer.ingester;

import com.danawa.fastcatx.indexer.Ingester;
import com.danawa.fastcatx.indexer.model.JdbcMetaData;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tomcat.util.http.fileupload.FileUtils;

import java.io.*;
import java.sql.*;
import java.util.*;


public class MultipleJDBCIngester implements Ingester {
    private static final String LOB_BINARY = "LOB_BINARY";
    private static final String LOB_STRING = "LOB_STRING";

    private int bulkSize;

    private Connection mainConnection;
    private Connection subConnection;

    private PreparedStatement mainPstmt;
    private PreparedStatement subPstmt;

    private ResultSet mainRs;
    private ResultSet subRs;

    private int lastQueryListCount;

    private int mainQueryListCount;
    private int subQueryListCount;

    private int columnCount;
    private String[] columnName;
    private String[] columnType;

    private Map<String, Object>[] dataSet;
    private List<Map<String, Object[]>> subDataSet;


    private List<File> tmpFile;

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


            logger.info("{}, {}, {}, {}", driverClassName, url, user, password);

            mainConnection = getConnection(driverClassName, url, user, password);
        }

        //쿼리 갯수 체크용 카운트
        lastQueryListCount = this.mainSqlList.size();

        if (jdbcMetaDataMap.containsKey("subJDBC")) { // 서브쿼리가 존재한다면..
            JdbcMetaData subJDBC = jdbcMetaDataMap.get("subJDBC");

            this.subDriverClassName = subJDBC.getDriverClassName();
            this.subUrl = subJDBC.getUrl();
            this.subUser = subJDBC.getUser();
            this.subPassword = subJDBC.getPassword();

            logger.info("{}, {}, {}, {}", subDriverClassName, subUrl, subUser, subPassword);

            subConnection = getConnection(subDriverClassName, subUrl, subUser, subPassword);

            this.subSqlList = sqlListMap.get("subSqlList");
            this.subSqlwhereclauseData = subSqlwhereclauseData;
        }

        dataSet = new Map[bulkSize];

        mainQueryListCount = 0;
        subQueryListCount = 0;

        //SQL 실행
        logger.info("dataSQL total_Count : {}", mainSqlList.size());

        executeQuery(mainQueryListCount);
    }

    public void executeQuery(int mainQueryListCount) throws IOException {
        try {
            if (mainQueryListCount > 0) {
                mainConnection = getConnection(driverClassName, url, user, password);
                subConnection = getConnection(subDriverClassName, subUrl, subUser, subPassword);
            }

            if (mainPstmt != null) {
                mainPstmt.close();
            }

            if (subPstmt != null) {
                subPstmt.close();
            }

            logger.info("Num-{} mainQuery Start", mainQueryListCount);

            if (fetchSize < 0) {
                //in mysql, fetch data row by row
                mainPstmt = mainConnection.prepareStatement(mainSqlList.get(mainQueryListCount), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                mainPstmt.setFetchSize(Integer.MIN_VALUE);
            } else {
                mainPstmt = mainConnection.prepareStatement(mainSqlList.get(mainQueryListCount));

                if (fetchSize > 0) {
                    // 인출할 행 수를 나타냅니다.
                    mainPstmt.setFetchSize(fetchSize);
                }
            }

            if (maxRows > 0) {
                // 최대 행 수를 나타내며, 제한이 없는 경우에는 0입니다.
                mainPstmt.setMaxRows(maxRows);
                subPstmt.setMaxRows(maxRows);
            }
            logger.info("메인색인쿼리 실행.");
            mainRs = mainPstmt.executeQuery(); // 메인색인쿼리 실행.

            ResultSetMetaData mainRsmd = mainRs.getMetaData();
            columnCount = mainRsmd.getColumnCount();
            columnName = new String[columnCount];
            columnType = new String[columnCount];

            for (int i = 0; i < columnCount; i++) {
                columnName[i] = mainRsmd.getColumnLabel(i + 1);
                String typeName = mainRsmd.getColumnTypeName(i + 1);
                columnType[i] = typeName;
                logger.info("Column-{} [{}]:[{}]", new Object[]{i + 1, columnName[i], typeName});
            }


        } catch (Exception e) {
            closeConnection();
            throw new IOException(e);
        }
    }

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
            fill();
            if (bulkCount == 0) {
                //다음 쿼리 실행
                mainQueryListCount++;

                if (hasNextQuery(mainQueryListCount, lastQueryListCount)) {
                    logger.info("next Query Start : {}", mainQueryListCount);
                    executeQuery(mainQueryListCount);
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
            fill();
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

    private void fill() throws IOException {
        bulkCount = 0;

        List<String> whereclauseList = new ArrayList<>();

        try {
            ResultSetMetaData mainRsmd = mainRs.getMetaData();

            while (mainRs.next()) {
                Map<String, Object> mainKeyValue = new HashMap<String, Object>();

                for (int idx = 0; idx < columnCount; idx++) {
                    int columnIdx = idx + 1;
                    int type = mainRsmd.getColumnType(columnIdx);

                    String str = "";

                    String lobType = null;
                    if (type == Types.BLOB || type == Types.BINARY || type == Types.LONGVARBINARY || type == Types.VARBINARY
                            || type == Types.JAVA_OBJECT) {
                        lobType = LOB_BINARY;
                    } else if (type == Types.CLOB || type == Types.NCLOB || type == Types.SQLXML || type == Types.LONGVARCHAR || type == Types.LONGNVARCHAR) {
                        lobType = LOB_STRING;
                    }

                    if (lobType == null) {
                        str = mainRs.getString(columnIdx);

                        if (str != null) {
                            mainKeyValue.put(columnName[idx], StringEscapeUtils.unescapeHtml(str));
                        } else {
                            // 파싱할 수 없는 자료형 이거나 정말 NULL 값인 경우
                            mainKeyValue.put(columnName[idx], "");
                        }
                    } else {
                        File file = null;

                        if (lobType == LOB_BINARY) {
                            // logger.debug("Column-"+columnIdx+" is BLOB!");
                            // BLOB일 경우 스트림으로 받는다.
                            ByteArrayOutputStream buffer = null;
                            try {
                                if (!useBlobFile) {
                                    buffer = new ByteArrayOutputStream();
                                }
                                file = readTmpBlob(idx, columnIdx, mainRsmd, buffer);
                                if (useBlobFile) {
                                    mainKeyValue.put(columnName[idx], file);
                                } else {
                                    mainKeyValue.put(columnName[idx], buffer.toByteArray());
                                }
                            } finally {
                                if (buffer != null) {
                                    try {
                                        buffer.close();
                                    } catch (IOException ignore) {

                                    }
                                }
                            }
                        } else if (lobType == LOB_STRING) {
                            StringBuilder sb = null;
                            if (!useBlobFile) {
                                sb = new StringBuilder();
                            }
                            file = readTmpClob(idx, columnIdx, mainRsmd, sb);
                            if (useBlobFile) {
                                mainKeyValue.put(columnName[idx], file);
                            } else {
                                mainKeyValue.put(columnName[idx], StringEscapeUtils.unescapeHtml(sb.toString()));
                            }
                        }

                        //다음 레코드 진행시 지우도록 한다.
                        if (file != null) {
                            tmpFile.add(file);
                        }

                    }
                    String mainColumnLabel = mainRsmd.getColumnLabel(columnIdx);
                    if (subSqlwhereclauseData.equals(mainColumnLabel)) {
                        if (mainRs.getObject(columnIdx) != null) {
                            whereclauseList.add(String.valueOf(mainRs.getObject(columnIdx))); // 써브쿼리 where 데이터
                        }

                    }
                }
                dataSet[bulkCount] = mainKeyValue;
                bulkCount++;
                totalCnt++;
                if (bulkCount >= bulkSize) {
                    break;
                }
            }

            if (whereclauseList.size() > 0) {
                String subQuery = subSqlList.get(subQueryListCount);
                subQuery = subQuery.replace("?", String.join(",", whereclauseList));

                fetchSize = whereclauseList.size();

                if (fetchSize < 0) {
                    subPstmt = subConnection.prepareStatement(subQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    subPstmt.setFetchSize(Integer.MIN_VALUE);

                } else {
                    subPstmt = subConnection.prepareStatement(subQuery);

                    if (fetchSize > 0) {
                        subPstmt.setFetchSize(fetchSize);
                    }
                }

                subRs = subPstmt.executeQuery(); // 서브색인쿼리 실행.

                ResultSetMetaData subRsmd = subRs.getMetaData();
                int subColumCount = subRsmd.getColumnCount(); // 서브쿼리 컬럼의 갯수를 반환한다.

                subBulkCount = 0;
                subDataSet = new ArrayList<>();

                while (subRs.next()) {
                    String subQueryProdCode = "";
                    String subQueryProdName = "";
                    String subQueryLowPrice = "";
                    int subQueryCategorySeq1 = 0;
                    int subQueryCategorySeq2 = 0;
                    int subQueryCategorySeq3 = 0;
                    int subQueryCategorySeq4 = 0;
                    Object [] subData = new Object[6];
                    for (int jdx = 1; jdx <= subColumCount; jdx++) {
                        String subColumnLabel = subRsmd.getColumnLabel(jdx);

                        if ("PRODUCTSEQ".equals(subColumnLabel)) {
                            subQueryProdCode = String.valueOf(subRs.getObject(jdx));
                        } else if ("PRODUCTNAME".equals(subColumnLabel)) {
                            subQueryProdName = String.valueOf(subRs.getObject(jdx));
                        } else if ("LOWPRICE".equals(subColumnLabel)) {
                            subQueryLowPrice = String.valueOf(subRs.getObject(jdx));
                        } else if ("CATEGORYSEQ1".equals(subColumnLabel)) {
                            subQueryCategorySeq1 = Integer.parseInt(String.valueOf(subRs.getObject(jdx)));
                        } else if ("CATEGORYSEQ2".equals(subColumnLabel)) {
                            subQueryCategorySeq2 = Integer.parseInt(String.valueOf(subRs.getObject(jdx)));
                        } else if ("CATEGORYSEQ3".equals(subColumnLabel)) {
                            subQueryCategorySeq3 = Integer.parseInt(String.valueOf(subRs.getObject(jdx)));
                        } else if ("CATEGORYSEQ4".equals(subColumnLabel)) {
                            subQueryCategorySeq4 = Integer.parseInt(String.valueOf(subRs.getObject(jdx)));
                        }
                    }


                    if (subQueryProdCode.length() > 0 && subQueryProdName.length() > 0) {
                        Map<String, Object[]> subKeyValue = new HashMap<>();

                        subData[0] = subQueryProdName;
                        subData[1] = subQueryLowPrice;
                        subData[2] = subQueryCategorySeq1;
                        subData[3] = subQueryCategorySeq2;
                        subData[4] = subQueryCategorySeq3;
                        subData[5] = subQueryCategorySeq4;

                        subKeyValue.put(subQueryProdCode, subData);

                        subDataSet.add(subKeyValue);

                    }

                }
            }
            for (Map<String, Object> mainData : dataSet) {
                if (mainData.containsKey(subSqlwhereclauseData)) {
                    String compareData = String.valueOf(mainData.get(subSqlwhereclauseData));

                    if (!compareData.equals("")) {
                        for (Map<String, Object[]> subData : subDataSet) {

                            if (subData.containsKey(compareData)) {
                                mainData.put("productName", subData.get(compareData)[0]);
                                mainData.put("lowPrice", subData.get(compareData)[1]);
                                mainData.put("categorySeq1", subData.get(compareData)[2]);
                                mainData.put("categorySeq2", subData.get(compareData)[3]);
                                mainData.put("categorySeq3", subData.get(compareData)[4]);
                                mainData.put("categorySeq4", subData.get(compareData)[5]);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("", e);

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

            throw new IOException(e);
        }
    }

    private File readTmpBlob(int columnInx, int columnNo, ResultSetMetaData rsMeta, OutputStream buffer) throws IOException, SQLException {
        File file = null;
        FileOutputStream os = null;
        InputStream is = null;
        try {
            is = mainRs.getBinaryStream(columnNo);
            if (is != null) {
                if (buffer == null) {
                    file = File.createTempFile("blob." + columnNo, ".tmp");
                    os = new FileOutputStream(file);
                }
                for (int rlen = 0; (rlen = is.read(data, 0, data.length)) != -1; ) {
                    if (buffer != null) {
                        buffer.write(data, 0, rlen);
                    } else {
                        os.write(data, 0, rlen);
                    }
                }
            }

        } catch (IOException e) {
            throw new IOException("Error while writing Blob field. column => " + rsMeta.getColumnName(columnNo));
        } finally {
            IOException ex = null;
            if (os != null)
                try {
                    os.close();
                } catch (IOException e) {
                    ex = e;
                }
            if (is != null)
                try {
                    is.close();
                } catch (IOException e) {
                    ex = e;
                }
            if (ex != null) {
                logger.error("Error while close LOB field and output file stream.", ex);
            }
        }
        return file;
    }

    private File readTmpClob(int columnInx, int columnNo, ResultSetMetaData rsMeta, StringBuilder buffer) throws IOException, SQLException {
        File file = null;
        BufferedWriter os = null;
        BufferedReader is = null;
        try {
            Reader reader = mainRs.getCharacterStream(columnNo);
            if (reader != null) {
                //buffer is null when using File
                if (buffer == null) {
                    file = File.createTempFile("clob." + columnNo, ".tmp");
                    os = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
                }
                is = new BufferedReader(reader);
                for (String rline = ""; (rline = is.readLine()) != null; ) {
                    if (buffer != null) {
                        buffer.append(rline).append("\n");
                    } else {
                        os.write(rline);
                        os.write("\n");
                    }
                }
            }
        } catch (IOException e) {
            throw new IOException("Error while writing Clob field. column => " + rsMeta.getColumnName(columnNo));
        } finally {
            IOException ex = null;
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    ex = e;
                }
            }
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    ex = e;
                }
            }
            if (ex != null) {
                logger.error("Error while close clob field and output file stream.", ex);
            }
        }
        return file;
    }

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
