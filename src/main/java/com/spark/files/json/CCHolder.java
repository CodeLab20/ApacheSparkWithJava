package com.spark.files.json;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.io.Serializable;
import java.sql.Timestamp;

public class CCHolder implements Serializable {
    private String name,email, city, mac, creditcard;

    //This timestamp format is set as timestamp is in format. (2015-04-25 13:57:36 +0700)
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss Z")
    private Timestamp timestamp;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public String getCreditcard() {
        return creditcard;
    }

    public void setCreditcard(String creditcard) {
        this.creditcard = creditcard;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public CCHolder() {
    }

    @Override
    public String toString() {
        return String.join(",", name, email, city,mac, creditcard, String.valueOf(timestamp));
    }
}
