package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class DatabaseQueryHelper {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseQueryHelper.class);

    private final long oneHour = 60 * 60 * 1000;

    public int getRowCount(Connection connection, String table) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(String.format("SELECT COUNT(*) FROM %s", table));
        ResultSet resultSet = preparedStatement.executeQuery();
        int count = 0;
        if (resultSet.next()) {
            count = resultSet.getInt(1);
        }
        return count;
    }

    public int delete(Connection connection, String tableName) throws SQLException {
        String deleteSql = String.format("DELETE FROM %s", tableName);
        return connection.createStatement().executeUpdate(deleteSql);
    }

    public ResultSet simpleSelect(Connection connection, String sql) throws SQLException {
        long st = System.currentTimeMillis();
        PreparedStatement preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet resultSet = preparedStatement.executeQuery();
        long nt = System.currentTimeMillis();
        if (sql.length() < 50){
            logger.info("Select ExecuteQuery. Elapsed time: {}ms  SQL: {}", nt - st, sql);
        } else {
            logger.info("Select ExecuteQuery. Elapsed time: {}ms  SQL: {}", nt - st, sql.substring(0, 50));
        }
        return resultSet;
    }

    public ResultSet simpleSelectForwadOnly(Connection connection, String sql) throws SQLException {
        long st = System.currentTimeMillis();
        PreparedStatement preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet resultSet = preparedStatement.executeQuery();
        long nt = System.currentTimeMillis();
        if (sql.length() < 50){
            logger.info("Select ExecuteQuery. Elapsed time: {}ms  SQL: {}", nt - st, sql);
        } else {
            logger.info("Select ExecuteQuery. Elapsed time: {}ms  SQL: {}", nt - st, sql.substring(0, 50));
        }
        return resultSet;
    }

    public boolean truncate(Connection connection, String tableName) throws SQLException {
        return truncate(connection, tableName, oneHour);
    }
    public boolean truncate(Connection connection, String tableName, long timeout) throws SQLException {
        boolean isTruncated = false;

        String truncatedCheckSql = String.format("SELECT COUNT(1) FROM %s", tableName);
        String truncateSql = "{CALL PRTRUNCATE(?, ?)}";
        String out = new String();
        CallableStatement callableStatement = connection.prepareCall(truncateSql);
        callableStatement.setString(1, tableName);
        callableStatement.setString(2, out);
        callableStatement.registerOutParameter(2, Types.CHAR);
        boolean result = callableStatement.execute();

        logger.info("Truncate Call. TableName: {}, result: {}, out: {}", tableName, result, out);

        long endTime = System.currentTimeMillis() + timeout;
        int n = 0;
        Set<Integer> infinityCheck = new HashSet<>();
        for (;System.currentTimeMillis() < endTime;) {
//            데이터가 별루 없으면 빨리 끝날거같아서,, 처음 10회는 1초마다 확인하고, 이후 부터는 5초 간격으로 갯수 확인한다.
            Utils.sleep(n++ < 10 ? 1000 : 5000);
            PreparedStatement preparedStatement = connection.prepareStatement(truncatedCheckSql);
            ResultSet resultSet = preparedStatement.executeQuery();
            if(resultSet.next()) {
                int count = resultSet.getInt(1);
                if(count == 0) {
                    isTruncated = true;
                    break;
                } else {
                    infinityCheck.add(count);
                }
            }
            if (n % 10 == 0) {
                logger.warn("Truncate Check Loop.. {}", n);
                if (infinityCheck.size() == 1) {
                    logger.warn("프로시져 호출 후 데이터 감소 안함.");
                    break;
                }
                infinityCheck.clear();
            }
        }
//            TODO 실패 시 알림 기능 추가

        logger.info("Truncated. tableName: {}", tableName);
        return isTruncated;
    }

}
