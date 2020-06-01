package com.danawa.fastcatx.indexer.impl;

import com.danawa.fastcatx.indexer.Ingester;
import org.apache.tomcat.util.http.fileupload.FileUtils;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCIngester implements Ingester {

    private static final String LOB_BINARY = "LOB_BINARY";
    private static final String LOB_STRING = "LOB_STRING";

    private int bulkSize;
    private Connection con;
    private PreparedStatement pstmt;
    private ResultSet r;
    private int columnCount;
    private String[] columnName;
    private Map<String, Object>[] dataSet;

    private List<File> tmpFile;

    private int bulkCount;
    private int readCount;

    private boolean useBlobFile;

    private boolean isClosed;

    private byte[] data = new byte[16 * 1024];
    private int totalCnt;


    public JDBCIngester(String dataSQL, int bulkSize, int fetchSize, int maxRows, boolean useBlobFile) throws IOException {
        this.bulkSize = bulkSize;
        this.useBlobFile = useBlobFile;
        tmpFile = new ArrayList<>();
        dataSet = new Map[bulkSize];

        try {
            if (fetchSize < 0) {
                //in mysql, fetch data row by row
                pstmt = con.prepareStatement(dataSQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                pstmt.setFetchSize(Integer.MIN_VALUE);
            } else {
                pstmt = con.prepareStatement(dataSQL);
                if (fetchSize > 0) {
                    pstmt.setFetchSize(fetchSize);
                }
            }

            if (maxRows > 0) {
                pstmt.setMaxRows(maxRows);
            }

            r = pstmt.executeQuery();

            ResultSetMetaData rsMetadata = r.getMetaData();
            columnCount = rsMetadata.getColumnCount();
            columnName = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columnName[i] = rsMetadata.getColumnLabel(i + 1).toUpperCase();
                String typeName = rsMetadata.getColumnTypeName(i + 1);
                logger.info("Column-{} [{}]:[{}]", new Object[]{i + 1, columnName[i], typeName});
            }
        } catch (Exception e) {
            closeConnection();

            throw new IOException(e);
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (readCount >= bulkCount) {
            fill();

            if (bulkCount == 0)
                return false;

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
        if(!isClosed) {
            logger.info("Close JDBCIngester.. Read {} docs.", totalCnt);
            deleteTmpLob();
            closeConnection();
            isClosed = true;
        }
    }

    private void closeConnection() {

        try {
            if (r != null) {
                r.close();
            }
        } catch (SQLException ignore) {
        }

        try {
            if (pstmt != null) {
                pstmt.close();
            }
        } catch (SQLException ignore) {
        }

        try {
            if (con != null && !con.isClosed()) {
                con.close();
            }
        } catch (SQLException ignore) {
        }
    }

    private void fill() throws IOException {

        bulkCount = 0;
        try {
            ResultSetMetaData rsMeta = null;
            //이전 Tmp 데이터들을 지워준다.
            deleteTmpLob();

            try {
                rsMeta = r.getMetaData();
            } catch (SQLException e) {
                return;
            }
            while (r.next()) {

                Map<String, Object> keyValueMap = new HashMap<String, Object>();

                for (int i = 0; i < columnCount; i++) {
                    int columnIdx = i + 1;
                    int type = rsMeta.getColumnType(columnIdx);

                    String str = "";

                    String lobType = null;
                    if (type == Types.BLOB || type == Types.BINARY || type == Types.LONGVARBINARY || type == Types.VARBINARY
                            || type == Types.JAVA_OBJECT) {
                        lobType = LOB_BINARY;
                    } else if (type == Types.CLOB || type == Types.NCLOB || type == Types.SQLXML || type == Types.LONGVARCHAR || type == Types.LONGNVARCHAR) {
                        lobType = LOB_STRING;
                    }

                    if(lobType == null) {
                        str = r.getString(columnIdx);

                        if(str != null) {
                            keyValueMap.put(columnName[i], str);
                        } else {
                            // 파싱할 수 없는 자료형 이거나 정말 NULL 값인 경우
                            keyValueMap.put(columnName[i], "");
                        }
                    } else {
                        File file = null;

                        if(lobType == LOB_BINARY) {
                            // logger.debug("Column-"+columnIdx+" is BLOB!");
                            // BLOB일 경우 스트림으로 받는다.
                            ByteArrayOutputStream buffer = null;
                            try {
                                if(!useBlobFile) {
                                    buffer = new ByteArrayOutputStream();
                                }
                                file = readTmpBlob(i, columnIdx, rsMeta, buffer);
                                if(useBlobFile) {
                                    keyValueMap.put(columnName[i], file);
                                } else {
                                    keyValueMap.put(columnName[i], buffer.toByteArray());
                                }
                            } finally {
                                if (buffer != null) {
                                    try {
                                        buffer.close();
                                    } catch (IOException ignore) {
                                    }
                                }
                            }
                        } else if(lobType == LOB_STRING) {
                            StringBuilder sb = null;
                            if(!useBlobFile) {
                                sb = new StringBuilder();
                            }
                            file = readTmpClob(i, columnIdx, rsMeta, sb);
                            if(useBlobFile) {
                                keyValueMap.put(columnName[i], file);
                            } else {
                                keyValueMap.put(columnName[i], sb.toString());
                            }
                        }

                        //다음 레코드 진행시 지우도록 한다.
                        if(file!=null) {
                            tmpFile.add(file);
                        }
                    }
                }

                dataSet[bulkCount] = keyValueMap;
                bulkCount++;
                totalCnt++;

                if (bulkCount >= bulkSize){
                    break;
                }
            }

        } catch (Exception e) {

            logger.debug("",e);

            try {
                if (r != null) {
                    r.close();
                }
            } catch (SQLException ignore) { }

            try {
                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException ignore) { }

            try {
                if (con != null && !con.isClosed()) {
                    con.close();
                }
            } catch (SQLException ignore) { }

            throw new IOException(e);
        }
    }

    private File readTmpBlob(int columnInx, int columnNo, ResultSetMetaData rsMeta, OutputStream buffer) throws IOException, SQLException {
        File file = null;
        FileOutputStream os = null;
        InputStream is = null;
        try {
            is = r.getBinaryStream(columnNo);
            if (is != null) {
                if(buffer == null) {
                    file = File.createTempFile("blob." + columnNo, ".tmp");
                    os = new FileOutputStream(file);
                    // logger.debug("tmp file = "+f.getAbsolutePath());
                }
                for (int rlen = 0; (rlen = is.read(data, 0, data.length)) != -1;) {
                    if(buffer != null) {
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

    private File readTmpClob (int columnInx, int columnNo, ResultSetMetaData rsMeta, StringBuilder buffer) throws IOException, SQLException {
        File file = null;
        BufferedWriter os = null;
        BufferedReader is = null;
        try {
            Reader reader = r.getCharacterStream(columnNo);
            if (reader != null) {
                //buffer is null when using File
                if(buffer == null) {
                    file = File.createTempFile("clob." + columnNo, ".tmp");
                    os = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
                }
                is = new BufferedReader(reader);
                for (String rline = ""; (rline = is.readLine()) != null;) {
                    if(buffer!=null) {
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
}
