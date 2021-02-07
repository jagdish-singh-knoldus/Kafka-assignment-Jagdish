package model;

public class User {
    private String id;
    private String name;
    private int age;
    private String course;

    //default constructor
    public User() {

    }

    //parameterized constructor to initialize values
    public User(String id, String name, int age, String course) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.course = course;
    }

    //methods to get values
    public String getID() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public int getAge() {
        return this.age;
    }

    public String getCourse() {
        return this.course;
    }

    //method to get string of values from object
    public String toString() {
        return "{id=" + id + ", name='" + name + "', age=" + age + ", course=" + course + "}";
    }
}