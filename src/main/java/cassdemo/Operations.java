package cassdemo;

import java.util.ArrayList;
import java.util.List;

import cassdemo.backend.BackendSession;
import cassdemo.types.Director;
import cassdemo.types.Employee;
import cassdemo.types.Skill;
import cassdemo.types.Task;

public class Operations {

    private BackendSession bs;
    public int[] listStats;
    public String[] descStats;
    /*
        List statystyk:
            0 - pracownik wykonywał odpowiednie zadanie po zakończeniu zadania
            1 - pracownik wykonywał inne zadanie
            2 - pracownik nie wykonywał żadnego zadania
            3 - zadanie które próbujemy usunąć lub zakończyć nie istnieje
            4 - pracownik pomimo że był wolny i odrazu go zarezerwowaliśmy to jednak został zarezerwowany przez kogoś innego - oznacza że wybieramy kogoś innego
            5 - jednak po skompletowaniu całej drużyny okazało się że pracownik jednak robi coś innego - oznacza że odrazu kończymy zadanie
            6 - task został poprawnie utworzony
            7 - task został anulowany bo brakuje kompetentnych praconwików
            8 - task został anulowany bo praconwik dostał przydział do innego zadania
            9 - task został zakończony w 100% poprawnie
            10 - podczas kończenia tasku okazało się że pracownicy byli przydzieleni do innych zadań
     */

    public Operations(String contactPoint, String keyspace) {
        try {
            this.bs = new BackendSession(contactPoint, keyspace);
            this.listStats = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
            this.descStats = new String[]{
              "Liczba pracowników którzy: Po zakończeniu zadania: Pracownik robił odpowiednie zadanie: \t",
              "Liczba pracowników którzy: Po zakończeniu zadania: Pracownik robił złe zadanie: \t\t",
              "Liczba pracowników którzy: Po zakończeniu zadania: Pracownik nie robił żadnego zadanie: \t",
              "Liczba pracowników którzy: Po zakończeniu zadania: Zadanie nie istnieje: \t\t\t",
              "Liczba pracowników którzy: Rezerwowanie pracownika: Pracownik dostał inne zadanie: \t\t",
              "Liczba pracowników którzy: Po skompletowaniu składu: Pracownik dostał inne zadanie: \t\t",
              "Liczba zadań które: Tworzenie składu: Poprawnie skompletowano skład drużyny: \t\t",
              "Liczba zadań które: Tworzenie składu: Anulowano kompletowanie składu, zabrakło specjalisty: \t",
              "Liczba zadań które: Tworzenie składu: Anulowano zadanie, ktoś dostał inne zadanie: \t\t",
              "Liczba zadań które: Kończenie taska: Wszysko było OK: \t\t\t\t\t",
              "Liczba zadań które: Kończenie taska: Ktoś robił inne zadanie: \t\t\t\t",
            };
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void initDatabase() {
        try {
            bs.createDirectorTable();
            bs.createEmployeeTable();
            bs.createSkillTable();
            bs.createTaskTable();
            bs.prepareStatements();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    
    public void addDirector(String name) {
        try {
            bs.upsertDirector(name);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public Director getDirector(String name) {
        try {
            return bs.getDirector(name);
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public String addEmployee(String name, int age, List<String> skills) {
        String employeeID;
        try {
            employeeID = bs.upsertEmployee(name, age, skills);
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }

        try {
            for (String skill: skills) {
                bs.upsertSkill(skill, employeeID);
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        return employeeID;
    }

    public void removeEmployeeTask(String employeeID, String taskID) {
        try {
            bs.deleteEmployeeFromTask(taskID, employeeID);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public Employee getEmployee(String employeeID) {
        try {
            return bs.getEmployee(employeeID);
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public List<Skill> getSkilledEmployees(String skill) {
        try {
            return bs.getSkills(skill);
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    public String addTask(String directorName, String taskName, String deadline, int peopleRequired, List<String> skillsRequired) {
        String taskID;
        try {
            taskID = bs.upsertTask(taskName, deadline, peopleRequired, skillsRequired);
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }

        try {
            bs.addDirectorTask(taskID, directorName);
        } catch (Exception e) {
            System.out.println(e);
        }

        return taskID;
    }

    public void assignTask(String taskID, String employeeID) {
        try {
            bs.addEmployeeToTask(taskID, employeeID);
        } catch (Exception e) {
            System.out.println(e);
            return;
        }

        try {
            bs.updateEmployeeTask(taskID, employeeID);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public Task getTask(String taskID) {
        try {
            return bs.getTask(taskID);
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    private boolean finishOrDeleteTask(boolean finish, String directorName, String taskID) {
        Task taskToFinish = getTask(taskID);
        if(taskToFinish == null) {
            incrementStatsList(3);
            return false;
        }
        List<String> employeesAssigned = taskToFinish.getEmployeeIdList();

        try {
            if(finish) bs.finishTask(taskID);
            else bs.deleteTask(taskID);
        } catch (Exception e) {
            System.out.println(e);
            return false;
        }

        try {
            bs.removeDirectorTask(taskID, directorName);
        } catch (Exception e) {
            System.out.println(e);
        }
        boolean everyEmployeeHasGoodTask = true;
        try {
            for (String employeeID: employeesAssigned) {
                String taskNow = getEmployeeTask(employeeID);
                if(taskNow == null) {
                    if(finish) incrementStatsList(2);
                    everyEmployeeHasGoodTask = false;
                }
                else if(!taskNow.equals(taskID)) {
                    if(finish) incrementStatsList(1);
                    everyEmployeeHasGoodTask = false;
                }
                else {
                    if(finish) incrementStatsList(0);
                    bs.updateEmployeeTask(null, employeeID);
                }

            }
        } catch (Exception e) {
            System.out.println(e);
        }
        return everyEmployeeHasGoodTask;
    }

    public void finishTask(String directorName, String taskID) {
        boolean result = finishOrDeleteTask(true, directorName, taskID);
        if(result) incrementStatsList(9);
        else incrementStatsList(10);
    }

    public void deleteTask(String directorName, String taskID) {
        finishOrDeleteTask(false, directorName, taskID);
    }

    public String getEmployeeTask(String employeeID) {
        Employee employee = getEmployee(employeeID);
        if(employee != null) return employee.getTaskId();
        return null;
    }

    public String addTaskAndAssignTaskToEmployees(String directorName, String taskName, String deadline, int peopleRequired, List<String> skillsRequired) {
        String taskID = addTask(directorName, taskName, deadline, peopleRequired, skillsRequired);
        int numberRequiredSkills = skillsRequired.size();
        List<String> listSelectedEmployee = new ArrayList<>();

        for (int i=0; i<peopleRequired; i++) {
            String skill = skillsRequired.get(i%numberRequiredSkills);
            List<Skill> listEmployees = getSkilledEmployees(skill);
            boolean employeeWasFound = false;
            for(Skill s : listEmployees) {
                String employeeID = s.getEmployeeId();
                if (getEmployeeTask(employeeID) != null) continue;
                assignTask(taskID, employeeID);
                String tID = getEmployeeTask(employeeID);
                if (tID != null && !tID.equals(taskID)) {
                    incrementStatsList(4);
                    removeEmployeeTask(employeeID, taskID);
                    continue;
                }
                listSelectedEmployee.add(employeeID);
                employeeWasFound = true;
                break;
            }
            if(!employeeWasFound) {
                deleteTask(directorName, taskID);
                incrementStatsList(7);
                return null;
            }
        }

        boolean employeeHasWrongTask = false;
        for(String employeeID : listSelectedEmployee) {
            String tID = getEmployeeTask(employeeID);
            if (tID != null && !tID.equals(taskID)) {
                incrementStatsList(5);
                employeeHasWrongTask = true;
            }
        }
        if(employeeHasWrongTask) {
            deleteTask(directorName, taskID);
            incrementStatsList(8);
            return null;
        }
        incrementStatsList(6);
        return taskID;
    }

    public synchronized void incrementStatsList(int index) {
        listStats[index] += 1;
    }
    
}
