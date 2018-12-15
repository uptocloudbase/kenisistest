package org.agd.entity;

public class Message {

    private final String feedId;
    private final String day;
    private final String month;
    private final String year;
    private final String name;
    private final int age;
    private final String postcode;

    public Message(String feedId, String day, String month, String year, String name, int age, String postcode) {
        this.feedId = feedId;
        this.day = day;
        this.month = month;
        this.year = year;
        this.name = name;
        this.age = age;
        this.postcode = postcode;
    }

    public String getDay() {
        return day;
    }

    public String getMonth() {
        return month;
    }

    public String getYear() {
        return year;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public String getPostcode() {
        return postcode;
    }

    public String getPartitionKey() {
        return day + "-" + month + "-" + year + "-" + feedId;
    }
}
