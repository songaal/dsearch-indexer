package com.danawa.fastcatx.indexer;

import java.io.File;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.github.fracpete.processoutput4j.output.ConsoleOutputProcessOutput;
import com.github.fracpete.rsync4j.RSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CallProcedure {

    Logger logger = LoggerFactory.getLogger(CallProcedure.class);

    private Connection connection;
    private CallableStatement cs = null;

    private String driverClassName;
    private String url;
    private String user;
    private String password;
    private String procedureName;
    private Integer groupSeq;
    private String path;
    private boolean isLink = false;

    public CallProcedure(String driverClassName, String url, String user, String password, String procedureName, Integer groupSeq, String path) {

        this.driverClassName = driverClassName;
        this.url = url;
        this.user = user;
        this.password = password;
        this.procedureName = procedureName;
        this.groupSeq = groupSeq;
        this.path = path;

    }

    public CallProcedure(String driverClassName, String url, String user, String password, String procedureName, Integer groupSeq, String path, boolean isLink) {

        this.driverClassName = driverClassName;
        this.url = url;
        this.user = user;
        this.password = password;
        this.procedureName = procedureName;
        this.groupSeq = groupSeq;
        this.path = path;
        this.isLink = isLink;

    }

    //프로시저 호출 따로
    public boolean callSearchProcedure() {

        try {
            connection =  getConnection(driverClassName, url, user, password);
            //프로시저 호출
            if(procedureName.length() > 0 &&  groupSeq != null) {
                cs = connection.prepareCall("{call " + procedureName + "(?,?)}");
                String exportFileName = "";
                if(isLink) exportFileName = "linkExt_" + groupSeq;
                else       exportFileName = "prodExt_" + groupSeq;

                cs.setInt(1, groupSeq);
                cs.setString(2, exportFileName);

                //프로시저 호출 소요시간 체크
                long start = System.nanoTime();
                logger.info("{} 프로시저 호출 : GROUPSEQ-{}, FileName-{}", procedureName, groupSeq, exportFileName);
                cs.execute();
                long end = System.nanoTime();
                logger.info("{} 프로시저 완료 : GROUPSEQ-{}, FileName-{}", procedureName, groupSeq, exportFileName);
                logger.info("프로시저 호출 시간 : {}", (end - start) / 1000000 + "ms");

                closeConnection();
            }
            return true;

        } catch (Throwable e) {
            logger.info("groupSeq: {}", groupSeq);
            logger.info("driverClassName: {}, url: {}, user: {}, password: {}", driverClassName, url, user, password);
            logger.error("", e);

            if (connection != null) {
                closeConnection();
            }
            logger.error(e.getMessage());
            return false;
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

    private void closeConnection() {

        try {
            if (cs != null && !cs.isClosed()) {
                cs.close();
            }
        } catch (SQLException throwables) {
        }

        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException ignore) {
        }
    }

}
