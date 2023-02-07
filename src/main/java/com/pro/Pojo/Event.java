package com.pro.Pojo;

public class Event {
    public String user;
    public String action;
    public Long times;

    public Event(String user, String action, Long times) {
        this.user = user;
        this.action = action;
        this.times = times;
    }

    public Event() {
    }

    @Override
    public String toString() {
        return "{" +
                "user='" + user + '\'' +
                ", action='" + action + '\'' +
                ", times=" + times +
                '}';
    }
}
