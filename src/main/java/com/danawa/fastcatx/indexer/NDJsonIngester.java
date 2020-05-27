package com.danawa.fastcatx.indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NDJsonIngester extends FileIngester {

    public NDJsonIngester(File file) {
        super(file);
    }

    @Override
    protected void initReader(BufferedReader reader) throws IOException {
        //do nothing
    }

    @Override
    protected Map<String, Object> parse(BufferedReader reader) throws IOException {
        String line = null;
        while ((line = reader.readLine()) != null) {
            Map<String, Object> record = new HashMap<String, Object>();
            try {
                String[] els = line.split(",");

                for (int i = 0; i < fieldIndexList.size(); i++) {
                    Integer index = fieldIndexList.get(i);
                    if (index != -1) {
                        record.put(fieldNameList.get(i), els[index]);
                    }
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
