package com.danawa.fastcatx.indexer.preProcess;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

public class DBTest {

    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("miss match. params >> driver, url, user, pass, table");
            System.out.println(Arrays.toString(args));
            return;
        }
        DatabaseConnector databaseConnector = new DatabaseConnector();
        databaseConnector.addConn(args[0], args[1], args[2], args[3]);
        try (Connection conn = databaseConnector.getConn()){

            DatabaseQueryHelper helper = new DatabaseQueryHelper();
            System.out.println(helper.truncate(conn, args[4]));

        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        }

    }

}
