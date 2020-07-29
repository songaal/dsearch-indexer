package com.danawa.fastcatx.indexer.output;

import com.github.fracpete.processoutput4j.output.AbstractProcessOutput;
import com.github.fracpete.processoutput4j.reader.ConsoleOutputProcessReader;

public class LogOutPutProcessOutput extends AbstractProcessOutput {


    @Override
    protected Thread configureStdErr(Process process) {
        return new Thread(new LogOutputProcessReader(process, false, ""));
    }

    @Override
    protected Thread configureStdOut(Process process) {
        return new Thread(new LogOutputProcessReader(process, true, ""));
    }
}
