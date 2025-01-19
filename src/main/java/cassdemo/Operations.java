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
        bs.createDirectorTable();
        bs.createEmployeeTable();
        bs.createSkillTable();
        bs.createTaskTable();
    }

    // return UUID of the added Director
    public String addDirector(String name) {
        try {
            return bs.upsertDirector(name);
        } catch (Exception e) {
            return null;
        }
    }

    public Director getDirector(String name) {
        try {
            return bs.getDirector(name);
        } catch (Exception e) {
            return null;
        }
    }

    public String addEmployee(String name, int age, List<String> skills) {
        String employeeID;
        try {
            employeeID = bs.upsertEmployee(name, age, skills);
        } catch (Exception e) {
            return null;
        }

        try {
            for (String skill: skills) {
                bs.upsertSkill(skill);
            }
        } catch (Exception e) {
        }

        return employeeID;
    }

    public Employee getEmployee(String empolyeeID) {
        try {
            return bs.getEmployee(employeeID);
        } catch (Exception e) {
            return null;
        }
    }

    public Skill getSkilledEmploees(String skill) {
        try {
            return bs.getSkills(skill);
        } catch (Exception e) {
            return null;
        }
    }

    public String addTask(String taskName, String deadline, int peopleRequired, List<String> skillsRequired) {
        String taskID;
        try {
            taskID = bs.upsertEmployee(name, age, skills);
        } catch (Exception e) {
            return null;
        }

        try {
            bs.addDirectorTask(taskID);
        } catch (Exception e) {
        }

        return taskID;
    }

    public void assignTask(String taskID, String employeeID) {
        try {
            bs.addEmployeeToTask(taskID, employeeID);
        } catch (Exception e) {
            return;
        }

        try {
            bs.updateEmployeeTask(taskID);
        } catch (Exception e) {
        }
    }

    public Task getTask(String taskID) {
        try {
            return bs.getTask(taskID);
        } catch (Exception e) {
            return null;
        }
    }

    public void finishTask(String taskID) {
        try {
            bs.finishTask(taskID);
        } catch (Exception e) {
            return null;
        }

        try {
            bs.removeDirectorTask(taskID);
        } catch (Exception e) {
        }

        try {
            bs.updateEmployeeTask(null);
        } catch (Exception e) {
        }
    }
    
}
