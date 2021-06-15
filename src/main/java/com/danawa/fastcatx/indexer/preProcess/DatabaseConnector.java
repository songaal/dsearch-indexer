package com.danawa.fastcatx.indexer.preProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseConnector implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnector.class);
    private final Map<String, Connection> connectionMap = new ConcurrentHashMap<>();
    private final Map<String, JdbcConfig> config = new ConcurrentHashMap<>();
    private final String DEFAULT_NAME = "DEFAULT_NAME";

    public boolean addConn(String alias, String driver, String url, String username, String password) {
        boolean isAdd = false;
        if (!connectionMap.containsKey(alias)) {
            JdbcConfig jdbcConfig = new JdbcConfig();
            jdbcConfig.setDriver(driver);
            jdbcConfig.setAddress(url);
            jdbcConfig.setUsername(username);
            jdbcConfig.setPassword(password);
            config.put(alias, jdbcConfig);
            isAdd = true;
        }
        return isAdd;
    }
    public boolean addConn(String driver, String url, String username, String password) {
        return addConn(DEFAULT_NAME, driver, url, username, password);
    }

    public Connection getConn(String alias) throws ClassNotFoundException, SQLException {
        JdbcConfig jdbcConfig = config.get(alias);
        if (!connectionMap.containsKey(DEFAULT_NAME) && jdbcConfig != null) {
            Class.forName(jdbcConfig.getDriver());
            connectionMap.put(alias,
                    DriverManager.getConnection(
                            jdbcConfig.getAddress(),
                            jdbcConfig.getUsername(),
                            jdbcConfig.getPassword()));
        }
        return connectionMap.get(alias);
    }
    public Connection getConn() throws SQLException, ClassNotFoundException {
        return getConn(DEFAULT_NAME);
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


    private static class JdbcConfig {
        private String driver;
        private String address;
        private String username;
        private String password;

        public String getDriver() {
            return driver;
        }

        public void setDriver(String driver) {
            this.driver = driver;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }
}
