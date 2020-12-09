package com.danawa.fastcatx.indexer.ingester;

import com.danawa.fastcatx.indexer.FileIngester;
import com.danawa.fastcatx.indexer.Utils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class ProcedureLinkIngester extends FileIngester {

    private Type entryType;
    private Gson gson;
    private String dumpFormat;
    private boolean isRun;
    private StringBuilder sb;

    public ProcedureLinkIngester(String filePath, String dumpFormat, String encoding, int bufferSize, int limitSize) {

        super(filePath, encoding, bufferSize, limitSize);
        logger.info("filePath : {}", filePath);
        gson = new Gson();
        entryType = new TypeToken<Map<String, Object>>() {}.getType();
        isRun = true;
        sb = new StringBuilder();

        this.dumpFormat = dumpFormat;

    }


    @Override
    protected void initReader(BufferedReader reader) {
        this.reader = reader;
        reset();
    }

    @Override
    protected Map<String, Object> parse(BufferedReader reader) throws IOException {
        String line = "";
        //종료 체크용 카운트
        while (isRun) {
            try {
                line = reader.readLine();
                if(line != null){
                    Map<String, Object> record = new HashMap<>();

                    if (dumpFormat.equals("konan")) {
                        record = gson.fromJson(Utils.convertKonanToNdJson(line), entryType);
                    } else if (dumpFormat.equals("ndjson")) {
                        record = gson.fromJson(line, entryType);
                    }

                    return record;
                }else{
                    stop();
                }
            } catch (Exception e) {
                logger.error("parsing error : line= " + line, e);
            }
        }
        throw new IOException("EOF");
    }


    public void reset() {isRun = true;}
    public void stop() {
        isRun = false;
    }

}
