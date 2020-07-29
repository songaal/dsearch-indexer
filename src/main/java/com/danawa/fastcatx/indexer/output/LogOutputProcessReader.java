package com.danawa.fastcatx.indexer.output;

import com.danawa.fastcatx.indexer.Ingester;
import com.github.fracpete.processoutput4j.reader.AbstractProcessReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogOutputProcessReader extends AbstractProcessReader {

    Logger logger = LoggerFactory.getLogger(LogOutputProcessReader.class);
    /** the default prefix for stdout. */
    public static String PREFIX_STDOUT = "[OUT] ";

    /** the default prefix for stderr. */
    public static String PREFIX_STDERR = "[ERR] ";

    /** the prefix to use. */
    protected String m_Prefix;

    public LogOutputProcessReader(Process process, boolean stdout) {
        this(process, stdout, null);
    }

    /**
     * Initializes the reader.
     *
     * @param process 	the process to monitor
     * @param stdout  	whether to read stdout or stderr
     * @param prefix	the prefix to use, null for auto-prefix
     */
    public LogOutputProcessReader(Process process, boolean stdout, String prefix) {
        super(process, stdout);
        m_Prefix = (prefix == null) ? (stdout ? PREFIX_STDOUT : PREFIX_STDERR) : prefix;
    }

    /**
     * Returns the prefix in use.
     *
     * @return		the prefix
     */
    public String getPrefix() {
        return m_Prefix;
    }

    /**
     * For processing the line read from stdout/stderr.
     *
     * @param line	the output line
     */
    @Override
    protected void process(String line) {
        if (m_Stdout)
            logger.info(m_Prefix + line);
        else
            logger.error(m_Prefix + line);
    }
}
