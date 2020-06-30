package spark.sql;

import java.io.Serializable;

/**
 * create by liuzhiwei on 2020/5/10
 */
public class People implements Serializable {
    private String name;
    private int age;

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
}
