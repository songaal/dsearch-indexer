package com.danawa.fastcatx.indexer.preProcess;

import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.sql.ResultSet;

@SpringBootTest
public class TruncateTest {


    @Test
    public void trunTest() {

        DatabaseConnector databaseConnector = new DatabaseConnector();
        databaseConnector.addConn(
                "Altibase.jdbc.driver.AltibaseDriver",
                "jdbc:Altibase://es2.danawa.io:30032/DNWALTI",
                "DBLINKDATA_A",
                "ektbfm#^^"
        );

        try (Connection masterConnection = databaseConnector.getConn()) {
            DatabaseQueryHelper databaseQueryHelper = new DatabaseQueryHelper();

            boolean result = databaseQueryHelper.truncate(masterConnection, "tFirstDateForSearch");
            System.out.println("result: " + result);
        } catch (Exception e){
            e.printStackTrace();
        }
    }


}
