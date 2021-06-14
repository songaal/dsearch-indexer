package com.danawa.fastcatx.indexer.preProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseConnector implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnector.class);
    private final Map<String, Connection> connectionMap = new ConcurrentHashMap<>();
    private final String DEFAULT_NAME = "DEFAULT_NAME";

    public boolean addConn(String alias, String driver, String url, String username, String password) throws ClassNotFoundException, SQLException {
        boolean isAdd = false;
        if (!connectionMap.containsKey(alias)) {
            Class.forName(driver);
            connectionMap.put(alias, DriverManager.getConnection(url, username, password));
            isAdd = true;
        }
        return isAdd;
    }
    public boolean addConn(String driver, String url, String username, String password) throws ClassNotFoundException, SQLException {
        boolean isAdd = false;
        if (!connectionMap.containsKey(DEFAULT_NAME)) {
            Class.forName(driver);
            connectionMap.put(DEFAULT_NAME, DriverManager.getConnection(url, username, password));
            isAdd = true;
        }
        return isAdd;
    }

    public Connection getConn(String name) {
        return connectionMap.get(name);
    }

    @Override
    public void close() throws IOException {
        connectionMap.values().forEach(connection -> {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (Exception e) {
                logger.warn("", e);
            }
        });
    }

}
