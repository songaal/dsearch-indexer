package com.danawa.fastcatx.indexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IndexerApplication implements CommandLineRunner {

    private static Logger logger = LoggerFactory.getLogger(IndexerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(IndexerApplication.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        logger.info("Indexer Application을 시작합니다!");
    }

}
