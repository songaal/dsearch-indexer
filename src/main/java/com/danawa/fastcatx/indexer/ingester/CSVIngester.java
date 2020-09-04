package com.danawa.fastcatx.indexer.ingester;

import com.danawa.fastcatx.indexer.FileIngester;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CSV 형식을 읽어들인다.
 * */
public class CSVIngester extends FileIngester {

    private List<String> headerList;

    public CSVIngester(String filePath, String encoding, int bufferSize) {
        this(filePath, encoding, bufferSize, 0);
    }

    public CSVIngester(String filePath, String encoding, int bufferSize, int limitSize) {
        super(filePath, encoding, bufferSize, limitSize);
    }

    @Override
    protected void initReader(BufferedReader reader) throws IOException {
        String headerLine = reader.readLine();
        String[] headers = headerLine.split(",");
        headerList = new ArrayList<String>();
        for (String header : headers) {
            headerList.add(header.trim().toUpperCase());
        }
    }

    @Override
    protected Map<String, Object> parse(BufferedReader reader) throws IOException {
            String line = null;
            while ((line = reader.readLine()) != null) {
                Map<String, Object> record = new HashMap<>();
                try {
                    String[] els = line.split(",");
                    if (els.length != headerList.size()) {
                        logger.error("parsing error skip.. {}", line);
                        continue;
                    }
                    for (int i = 0; i < headerList.size(); i++) {
                        // HTML Decode
                        record.put(headerList.get(i), StringEscapeUtils.unescapeHtml(els[i]));
                    }
                    //정상이면 리턴.
                    return record;
                }catch(Exception e) {
                    logger.error("parsing error : line= " + line, e);
                }
            }
            throw new IOException("EOF");
    }

}
