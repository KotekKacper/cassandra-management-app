package cassdemo.types;

import java.util.List;

public class Task {
    private String taskId;
    private List<String> employeeIdList;
    private String name;
    private String deadline;
    private boolean finished;
    private int peopleRequired;
    private List<String> skillsRequired;

    public Task(String taskId, String employeeId, String name, String deadline, boolean finished, int peopleRequired, List<String> skillsRequired) {
        this.taskId = taskId;
        this.employeeIdList = employeeIdList != null ? employeeIdList : new ArrayList<>();
        this.name = name;
        this.deadline = deadline;
        this.finished = finished;
        this.peopleRequired = peopleRequired;
        this.skillsRequired = skillsRequired;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public List<String> getEmployeeIdList() {
        return employeeIdList;
    }

    public void setEmployeeIdList(List<String> employeeIdList) {
        this.employeeIdList = employeeIdList;
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDeadline() {
        return deadline;
    }

    public void setDeadline(String deadline) {
        this.deadline = deadline;
    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished(boolean finished) {
        this.finished = finished;
    }

    public int getPeopleRequired() {
        return peopleRequired;
    }

    public void setPeopleRequired(int peopleRequired) {
        this.peopleRequired = peopleRequired;
    }

    public List<String> getSkillsRequired() {
        return skillsRequired;
    }

    public void setSkillsRequired(List<String> skillsRequired) {
        this.skillsRequired = skillsRequired;
    }
}
