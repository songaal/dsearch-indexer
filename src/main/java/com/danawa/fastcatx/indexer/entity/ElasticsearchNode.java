package com.danawa.fastcatx.indexer.entity;

import java.io.Serializable;

public class ElasticsearchNode implements Serializable {

    private String host;
    private int port;
    private String scheme;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

}
