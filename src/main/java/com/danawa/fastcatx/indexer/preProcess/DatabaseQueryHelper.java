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
        resultSet.close();
        return count;
    }

    public int delete(Connection connection, String tableName) throws SQLException {
        String deleteSql = String.format("DELETE FROM %s", tableName);
        return connection.createStatement().executeUpdate(deleteSql);
    }

    public ResultSet simpleSelect(Connection connection, String sql) throws SQLException {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        long st = System.currentTimeMillis();
        preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        resultSet = preparedStatement.executeQuery();
        long nt = System.currentTimeMillis();
        if (sql.length() < 50){
            logger.info("Select ExecuteQuery. Elapsed time: {}ms  SQL: {}", nt - st, sql);
        } else {
            logger.info("Select ExecuteQuery. Elapsed time: {}ms  SQL: {}", nt - st, sql.substring(0, 50));
        }
        return resultSet;
    }

    public ResultSet simpleSelectForwadOnly(Connection connection, String sql) throws SQLException {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        long st = System.currentTimeMillis();
        preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        resultSet = preparedStatement.executeQuery();
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
        callableStatement.registerOutParameter(2, Types.VARCHAR);
        boolean result = callableStatement.execute();
        logger.info("Truncate Call. TableName: {}, result: {}, out: {}", tableName, result, out);
        long endTime = System.currentTimeMillis() + timeout;
        int n = 0;
        int prevRowSize = 0;
        final int CHECK_COUNT = 5;
        int checkCountDown = CHECK_COUNT;
        int retryTruncateCount = 0;
        for (;System.currentTimeMillis() < endTime;) {
//            ???????????? ?????? ????????? ?????? ??????????????????,, ?????? 10?????? 1????????? ????????????, ?????? ????????? 5??? ???????????? ?????? ????????????.
            Utils.sleep(n++ < 10 ? 1000 : 5000);
            PreparedStatement preparedStatement = connection.prepareStatement(truncatedCheckSql);
            ResultSet resultSet = null;
            try {
                resultSet = preparedStatement.executeQuery();
                if(resultSet.next()) {
                    int count = resultSet.getInt(1);
                    if(count == 0) {
                        isTruncated = true;
                        break;
                    } else if (n == 1) {
                        prevRowSize = count;
                    } else if (prevRowSize <= count) {
                        checkCountDown --;
                        logger.warn("???????????? ?????? ??? ????????? ?????? ??????. ?????? ??????: {}, ?????? ??????: {}", prevRowSize, count);
                        if (checkCountDown == 0) {
                            n = 0;
                            checkCountDown = CHECK_COUNT;
                            callableStatement.execute();
                            logger.info("procedure truncate retry.. {} TableName: {}, result: {}, out: {}", ++retryTruncateCount, tableName, result, out);
                            Utils.sleep(1000);
                        }
                    } else {
                        prevRowSize = count;
                    }
                }
                if (n % 10 == 0) {
                    logger.info("Truncate Check Loop.. {}", n);
                }
            } catch (SQLException e) {
                logger.error(e.getMessage());
            } finally {
                resultSet.close();
            }
        }

        if (checkCountDown > 0) {
            logger.info("Truncated. tableName: {}. out: {}", tableName, out);
        } else {
            logger.info("Truncated fail !!!!!!!. tableName: {}. out: {}", tableName, out);
        }
        return isTruncated;
    }
}