package com.pro.Pojo;

import java.util.HashMap;

public class ActionAggInfo {
    public String action;
    public Integer actioninfo;
    public Long start;
    public Long end;

    @Override
    public String toString() {
        return "ActionAggInfo{" +
                "action='" + action + '\'' +
                ", actioninfo=" + actioninfo +
                ", start=" + start +
                ", end=" + end +
                '}';
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Integer getActioninfo() {
        return actioninfo;
    }

    public void setActioninfo(Integer actioninfo) {
        this.actioninfo = actioninfo;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    public ActionAggInfo(String action, Integer actioninfo, Long start, Long end) {
        this.action = action;
        this.actioninfo = actioninfo;
        this.start = start;
        this.end = end;
    }

    public ActionAggInfo() {
    }
}
