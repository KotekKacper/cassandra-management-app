package cassdemo.types;

import java.util.List;

public class Employee {
    private String employeeId;
    private String name;
    private int age;
    private List<String> skills;
    private String taskId;

    public Employee(String employeeId, String name, int age, List<String> skills, String taskId) {
        this.employeeId = employeeId;
        this.name = name;
        this.age = age;
        this.skills = skills;
        this.taskId = taskId;
    }

    public String getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(String employeeId) {
        this.employeeId = employeeId;
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

    public List<String> getSkills() {
        return skills;
    }

    public void setSkills(List<String> skills) {
        this.skills = skills;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
}
