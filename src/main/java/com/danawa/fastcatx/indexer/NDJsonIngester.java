package com.danawa.fastcatx.indexer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * Newline Delimiter Json 형식을 읽어들인다.
 * */
public class NDJsonIngester extends FileIngester {

    private Type entryType;
    private Gson gson;

    public NDJsonIngester(String filePath, String encoding, int bufferSize) {
        this(filePath, encoding, bufferSize, 0);
    }

    public NDJsonIngester(String filePath, String encoding, int bufferSize, int limitSize) {
        super(filePath, encoding, bufferSize, limitSize);
        gson = new Gson();
        entryType = new TypeToken<Map<String, Object>>() {}.getType();
    }

    @Override
    protected void initReader(BufferedReader reader) throws IOException {
        //do nothing
    }

    @Override
    protected Map<String, Object> parse(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            try {
                Map<String, Object> record = gson.fromJson(line, entryType);
                //정상이면 리턴.
                return record;
            }catch(Exception e) {
                logger.error("parsing error : line= " + line, e);
            }
        }
        throw new IOException("EOF");
    }
}
