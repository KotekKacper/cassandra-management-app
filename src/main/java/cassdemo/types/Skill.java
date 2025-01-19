package cassdemo.types;

public class Skill {
    private String skillName;
    private String employeeId;

    public Skill(String skillName, String employeeId) {
        this.skillName = skillName;
        this.employeeId = employeeId;
    }

    public String getSkillName() {
        return skillName;
    }

    public void setSkillName(String skillName) {
        this.skillName = skillName;
    }

    public String getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(String employeeId) {
        this.employeeId = employeeId;
    }
}
