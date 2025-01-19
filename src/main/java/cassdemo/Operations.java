package cassdemo;

import cassdemo.types.Director;
import cassdemo.types.Employee;
import cassdemo.types.Skill;
import cassdemo.types.Task;

public class Operations {

    private BackendSession bs;


    public Operations(String contactPoint, String keyspace) {
        this.bs = new BackendSession(contact_point, keyspace);

    }

    public void initDatabase() {
        // Add clear table Director


        // Add clear table Employee


        // Add clear table Skill


        // Add clear table Task

    }

    public String addDirector(String name) {
        // Inserting a director

    }

    public getDirector(String name) {
        // Selecting a director

    }

    public String addEmployee(String name, int age, List<String> skills) {
        // Inserting an employee


        // Inserting skills

    }

    public getEmployee() {
        // Selecting an employee

    }

    public getSkilledEmploees(String skill) {
        // Selecting employees with a given skill

    }

    public String addTask(String taskName, String deadline, int peopleRequired, List<String> skillsRequired) {
        // Inserting a task

        // Updating a director

    }

    public void assignTask(String taskID, String employeeID) {
        // Inserting a task

        // Updating an employee

    }

    public getTask() {
        // Selecting a task

    }

    public void finishTask(String taskID) {
        // Updating tasks

        // Updating a director

        // Updating an employee

    }
    
}
