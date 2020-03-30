package com.yyw.pojo;

import java.io.Serializable;

public class People implements Serializable {
    private int id;
    private String name;
    private int age;
    private String category;
    private String position;

    public People() {
    }

    public People(int id, String name, int age, String category, String position) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.category = category;
        this.position = position;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }
}
