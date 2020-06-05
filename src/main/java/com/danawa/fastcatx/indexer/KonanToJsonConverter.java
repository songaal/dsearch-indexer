package com.danawa.fastcatx.indexer;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KonanToJsonConverter implements FileFilter {

    private static Logger logger = LoggerFactory.getLogger(KonanToJsonConverter.class);

    private String path;
    private String encoding;
    private int count;
    List<File> files;
    private static final Pattern ptnHead = Pattern.compile("\\x5b[%]([a-zA-Z0-9_-]+)[%]\\x5d");
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

    public KonanToJsonConverter(String path, String encoding) {
        this.path = path;
        this.encoding = encoding;
    }

    public void convert(String outputFilePath) throws IOException {
        count = 0;
        files = new ArrayList<>();
        String[] paths = path.split(",");
        for (String path : paths) {
            path = path.trim();
            File base = new File(path);
            if (!base.exists()) {
                logger.debug("BASE FILE NOT FOUND : {}", base);
            } else {
                if (base.isDirectory()) {
                    base.listFiles(this);
                } else {
                    files.add(base);
                }
            }
        }

        if (files.size() > 0) {

            File outputFile = new File(outputFilePath);
            StringWriter writer = null;
            Charset cs = Charset.forName("utf-8");
            BufferedWriter fileWriter= new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), cs));
            BufferedReader reader = null;
            InputStream istream = null;
            long time = System.currentTimeMillis();
            try {

                JsonGenerator generator = null;
                for (File file : files) {
                    if (!file.exists()) {
                        logger.debug("FILE NOT FOUND : {}", file);
                        continue;
                    }

                    istream =  new FileInputStream(file);
                    reader = new BufferedReader(new InputStreamReader(istream, String.valueOf(encoding)));
                    logger.debug("PARSING FILE..{}", file);
                    for (String line; (line = reader.readLine()) != null; count++) {
                        boolean isSourceFile = false;
                        Matcher mat = ptnHead.matcher(line);
                        String key = null;
                        int offset = 0;

                        while (mat.find()) {
                            if (!isSourceFile) {
                                //row 처음에 한번만 실행.
                                writer = new StringWriter();
                                generator = new JsonFactory().createGenerator(writer);
                                generator.writeStartObject();
                            }
                            isSourceFile = true;
                            if (key != null) {
                                String value = line.substring(offset, mat.start()).trim();
                                if (key.equals("")) {
                                    logger.error("ERROR >> {}:{}", key, value);
                                }
                                logger.debug("{} > {}", key, value);
                                generator.writeStringField(key, value);
                            }
                            key = mat.group(1);
                            offset = mat.end();
                        }
                        if (isSourceFile) {
                            String value = line.substring(offset);
                            generator.writeStringField(key, value);
                            generator.writeEndObject();
                            generator.close();
                            fileWriter.write(writer.toString());
                            fileWriter.write("\n");

                            if (count > 0 && count % 100000 == 0) {
                                logger.info("{} ROWS FLUSHED! in {}ms", count, System.currentTimeMillis() - time);
                            }
                        } else {
                            logger.debug("{} IS NOT SOURCEFILE", file);
                            // 소스파일이 아니므로 바로 다음파일로.
                            break;
                        }
                    }
                    try { reader.close(); } catch (Exception ignore) { }
                }
                logger.info("TOTAL {} ROWS in {}ms", count, System.currentTimeMillis() - time);
            } catch (Exception e) {
                logger.error("", e);
            } finally {
                try { reader.close(); } catch (Exception ignore) { }
                try { fileWriter.close(); } catch (Exception ignore) { }
            }
        } else {
            logger.debug("THERE'S NO SOURCE FILE(S) FOUND");
        }
    }

    @Override public boolean accept(File file) {
        if (!file.exists()) { return false; }
        if (file.isDirectory()) {
            file.listFiles(this);
        } else if (file.isFile()) {
            files.add(file);
        }
        return false;
    }

    public static void main(String[] args) throws IOException {
        String path = args[0];
        String encoding = args[1];
        String outputFilePath = args[2];
        new KonanToJsonConverter(path, encoding).convert(outputFilePath);
    }
}
