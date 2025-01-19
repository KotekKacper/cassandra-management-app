package cassdemo;

import java.util.List;

import cassdemo.backend.BackendSession;
import cassdemo.types.Director;
import cassdemo.types.Employee;
import cassdemo.types.Skill;
import cassdemo.types.Task;

public class Operations {

    private BackendSession bs;

    public Operations(String contactPoint, String keyspace) {
        try {
            this.bs = new BackendSession(contactPoint, keyspace);
        } catch (Exception e) {
        }
    }

    public void initDatabase() {
        try {
            bs.createDirectorTable();
            bs.createEmployeeTable();
            bs.createSkillTable();
            bs.createTaskTable();
        } catch (Exception e) {
        }
    }

    // return UUID of the added Director
    public void addDirector(String name) {
        try {
            bs.upsertDirector(name);
        } catch (Exception e) {
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
                bs.upsertSkill(skill, employeeID);
            }
        } catch (Exception e) {
        }

        return employeeID;
    }

    public Employee getEmployee(String employeeID) {
        try {
            return bs.getEmployee(employeeID);
        } catch (Exception e) {
            return null;
        }
    }

    public List<Skill> getSkilledEmploees(String skill) {
        try {
            return bs.getSkills(skill);
        } catch (Exception e) {
            return null;
        }
    }

    public String addTask(String directorName, String taskName, String deadline, int peopleRequired, List<String> skillsRequired) {
        String taskID;
        try {
            taskID = bs.upsertTask(taskName, deadline, peopleRequired, skillsRequired);
        } catch (Exception e) {
            return null;
        }

        try {
            bs.addDirectorTask(taskID, directorName);
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
            bs.updateEmployeeTask(taskID, employeeID);
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

    public void finishTask(String directorName, String taskID) {
        Task taskToFinish = getTask(taskID);
        List<String> employeesAssigned = taskToFinish.getEmployeeIdList();

        try {
            bs.finishTask(taskID);
        } catch (Exception e) {
            return;
        }

        try {
            bs.removeDirectorTask(taskID, directorName);
        } catch (Exception e) {
        }

        try {
            for (String employeeID: employeesAssigned) {
                bs.updateEmployeeTask(null, employeeID);
            }
        } catch (Exception e) {
        }
    }
    
}
