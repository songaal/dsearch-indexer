package com.danawa.fastcatx.indexer.ingester;

import com.danawa.fastcatx.indexer.FileIngester;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DelimiterFileIngester extends FileIngester {

    private String delimiter;
    private String[] headerArr;

    public DelimiterFileIngester(String filePath, String encoding, int bufferSize, String headerText, String delimiter) {
        this(filePath, encoding, bufferSize, 0, headerText, delimiter);
    }

    public DelimiterFileIngester(String filePath, String encoding, int bufferSize, int limitSize, String headerText, String delimiter) {
        super(filePath, encoding, bufferSize, limitSize);
        this.delimiter = delimiter;

        headerArr = headerText.split(",");


    }
    @Override
    protected void initReader(BufferedReader reader) throws IOException {

    }

    @Override
    protected Map<String, Object> parse(BufferedReader reader) throws IOException {
        String line = null;
        while ((line = reader.readLine()) != null) {

            Map<String, Object> record = new HashMap<>();
            try {
                String[] els = line.split(delimiter);

                if (els.length != headerArr.length) {
                    logger.error("parsing error skip.. {}", line);
                    continue;
                }
                for (int i = 0; i < headerArr.length; i++) {
                    record.put(headerArr[i], els[i]);
                }
                //정상이면 리턴
                return record;
            }catch(Exception e) {
                logger.error("parsing error : line= " + line, e);
            }
        }
        throw new IOException("EOF");
    }
}
