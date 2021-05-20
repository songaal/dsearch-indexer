package com.danawa.fastcatx.indexer.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Job implements Serializable {
    private UUID id;
    private Map<String, Object> request;
    private String status;
    private String error;
    private long startTime;
    private long endTime;
    private String action;

    private Set<Integer> groupSeq = Collections.synchronizedSet(new LinkedHashSet<>());

    @JsonIgnore
    private Boolean stopSignal;

    public Map<String, Object> getRequest() {
        return request;
    }

    public void setRequest(Map<String, Object> request) {
        this.request = request;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Boolean getStopSignal() {
        return stopSignal;
    }

    public void setStopSignal(Boolean stopSignal) {
        this.stopSignal = stopSignal;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Set<Integer> getGroupSeq() {
        return groupSeq;
    }

    public void setGroupSeq(Set<Integer> groupSeq) {
        this.groupSeq = groupSeq;
    }
}
