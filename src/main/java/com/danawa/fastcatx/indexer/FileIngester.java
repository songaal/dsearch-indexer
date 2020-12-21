package com.danawa.fastcatx.indexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public abstract class FileIngester implements Ingester {

    private LinkedList<Map<String, Object>> items;
    protected String encoding;
    protected int bufferSize;
    protected int limitSize;
    protected BufferedReader reader;
    private static final int DEFAULT_BUFFER_SIZE = 100;
    private int readCount;
    private List<File> files;

    public FileIngester(String filePath, String encoding) {
        this(filePath, encoding, DEFAULT_BUFFER_SIZE, 0);
    }

    public FileIngester(String filePath, String encoding, int bufferSize, int limitSize) {
        this.encoding = encoding;
        this.bufferSize = bufferSize;
        this.limitSize = limitSize;

        files = new LinkedList<>();
        String[] paths = filePath.split(",");
        for (String path : paths) {
            path = path.trim();
            File base = new File(path);
            if (!base.exists()) {
                logger.debug("BASE FILE NOT FOUND : {}", base);
            } else {
                if (base.isDirectory()) {
                    base.listFiles(new FileFilter (){
                        @Override
                        public boolean accept(File file) {
                            if (!file.exists()) { return false; }
                            if (file.isDirectory()) {
                                file.listFiles(this);
                            } else if (file.isFile()) {
                                files.add(file);
                            }
                            return false;
                        }
                    });
                } else {
                    files.add(base);
                }
            }
        }
        items = new LinkedList<>();
    }

    public FileIngester() {

    }

    @Override
    public void close() throws IOException {
        if(reader != null) {
            try {
                logger.info("EXIT");
                reader.close();
                Thread.interrupted();
            } catch (IOException ignore) {
            }
        }
    }

    protected abstract void initReader(BufferedReader reader) throws IOException;

    private void fill() throws IOException {
        while (true) {
            if(reader != null) {
                try {
                    if(items.size() >= bufferSize) {
                        return;
                    }
                    if(limitSize > 0 && readCount >= limitSize) {
                        return;
                    }
                    Map<String, Object> record = parse(reader);
                    items.addLast(record);
                    readCount++;
                } catch(IOException e) {
                    //get next reader..
                    try {
                        reader.close();
                    } catch (IOException ignore) { }
                    reader = null;
                    continue;
                }
            } else {
                while (files.size() > 0) {
                    File f = files.remove(0);
                    if(!f.exists()) {
                        //파일이 없으면 continue
                        logger.error(String.format("File not exists : %s", f.getAbsolutePath()));
                        continue;
                    }
                    try {
                        if(isGZipped(f)) {
                            reader = new BufferedReader((new InputStreamReader(new GZIPInputStream(new FileInputStream(f)), encoding)));
                        } else {
                            reader = new BufferedReader(new InputStreamReader(new FileInputStream(f), encoding));
                        }
                        logger.info("{} start / remain count : {}", f.getAbsolutePath(), files.size());
                        initReader(reader);
                        break;
                    } catch (IOException ex) {
                        logger.error("", ex);
                        if(reader != null) {
                            try {
                                reader.close();
                            } catch (IOException ignore) {
                            }
                            reader = null;
                        }
                    }
                }
                //파일이 더 이상 없으면 끝낸다.
                if(reader == null) {
                    break;
                }
            }
        }
    }

    private boolean isGZipped(File file) {
        int magic = 0;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "r");
            magic = raf.read() & 0xff | ((raf.read() << 8) & 0xff00);
        } catch (Throwable t) {
            logger.error("error while inspect file header.", t);
        } finally {
            if(raf != null) {
                try {
                    raf.close();
                } catch (IOException ignore) {
                }
            }
        }
        return magic == GZIPInputStream.GZIP_MAGIC;
    }

    protected abstract Map<String, Object> parse(BufferedReader reader) throws IOException;

    public boolean hasNext() throws IOException {
        if(items.size() == 0) {
            fill();
        }
        return items.size() > 0;
    }

    public Map<String, Object> next() throws IOException {
        if(items.size() == 0) {
            fill();
        }
        if(items.size() > 0) {
            return items.removeFirst();
        }
        return null;
    }
}
