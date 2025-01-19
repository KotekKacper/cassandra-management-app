package cassdemo.backend;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import cassdemo.types.Director;
import cassdemo.types.Employee;
import cassdemo.types.Skill;
import cassdemo.types.Task;


public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	private Session session;

	private static PreparedStatement DROP_TABLE_DIRECTOR;
	private static PreparedStatement DROP_TABLE_EMPLOYEE;
	private static PreparedStatement DROP_TABLE_SKILL;
	private static PreparedStatement DROP_TABLE_TASK;
	private static PreparedStatement CREATE_TABLE_DIRECTOR;
	private static PreparedStatement CREATE_TABLE_EMPLOYEE;
	private static PreparedStatement CREATE_TABLE_SKILL;
	private static PreparedStatement CREATE_TABLE_TASK;
	private static PreparedStatement INSERT_INTO_DIRECTOR;
	private static PreparedStatement SELECT_DIRECTOR;
	private static PreparedStatement ADD_DIRECTOR_TASK;
	private static PreparedStatement REMOVE_DIRECTOR_TASK;
	private static PreparedStatement INSERT_INTO_EMPLOYEE;
	private static PreparedStatement SELECT_EMPLOYEE;
	private static PreparedStatement UPDATE_EMPLOYEE_TASK;
	private static PreparedStatement INSERT_INTO_SKILL;
	private static PreparedStatement SELECT_SKILL;
	private static PreparedStatement INSERT_TASK;
	private static PreparedStatement ADD_EMPLOYEE_TO_TASK;
	private static PreparedStatement SELECT_TASK;
	private static PreparedStatement FINISH_TASK;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareInitialStatements();
	}

	private void prepareInitialStatements() throws BackendException {
		try {
			DROP_TABLE_DIRECTOR = session.prepare("DROP TABLE IF EXISTS Director;");
			DROP_TABLE_EMPLOYEE = session.prepare("DROP TABLE IF EXISTS Employee;");
			DROP_TABLE_SKILL = session.prepare("DROP TABLE IF EXISTS Skill;");
			DROP_TABLE_TASK = session.prepare("DROP TABLE IF EXISTS Task;");
			CREATE_TABLE_DIRECTOR = session.prepare("CREATE TABLE Director (name text, tasks list<text>, PRIMARY KEY (name));");
			CREATE_TABLE_EMPLOYEE = session.prepare("CREATE TABLE Employee (employee_id text, name text, age int, skills list<text>, task_id text, PRIMARY KEY (employee_id));");
			CREATE_TABLE_SKILL = session.prepare("CREATE TABLE Skill (skill_name text, employee_id text, PRIMARY KEY ((skill_name), employee_id));");
			CREATE_TABLE_TASK = session.prepare("CREATE TABLE Task (task_id text, employee_id text, name text, deadline text, finished boolean, people_required int, skills_required list<text>, PRIMARY KEY (task_id));");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Initial Statements prepared");
	}

	public void prepareStatements() throws BackendException {
		try {
			// SELECT_COLUMNS = session.prepare("SELECT * FROM x;").setConsistencyLevel(ConsistencyLevel.ONE);
			INSERT_INTO_DIRECTOR = session.prepare("INSERT INTO Director (name) VALUES (?);");
			SELECT_DIRECTOR = session.prepare("SELECT * FROM Director WHERE name = ?;");
			ADD_DIRECTOR_TASK = session.prepare("UPDATE Director SET tasks = tasks + ? WHERE name = ?;");
			REMOVE_DIRECTOR_TASK = session.prepare("UPDATE Director SET tasks = tasks - ? WHERE name = ?;");
			INSERT_INTO_EMPLOYEE = session.prepare("INSERT INTO Employee (employee_id, name, age, skills) VALUES (?, ?, ?, ?);");
			SELECT_EMPLOYEE = session.prepare("SELECT * FROM Employee WHERE employee_id = ?;");
			UPDATE_EMPLOYEE_TASK = session.prepare("UPDATE Employee SET task_id = ? WHERE employee_id = ?;");
			INSERT_INTO_SKILL = session.prepare("INSERT INTO Skill (skill_name, employee_id) VALUES (?, ?);");
			SELECT_SKILL = session.prepare("SELECT * FROM Skill WHERE skill_name = ?;");
			INSERT_TASK = session.prepare("INSERT INTO Task (task_id, name, deadline, people_required, skills_required) VALUES (?, ?, ?, ?, ?);");
			ADD_EMPLOYEE_TO_TASK = session.prepare("INSERT INTO Task (task_id, employee_id) VALUES (?, ?);");
			SELECT_TASK = session.prepare("SELECT * FROM Task WHERE task_id = ?;");
			FINISH_TASK = session.prepare("UPDATE Task SET finished = true WHERE task_id = ?;");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public void createDirectorTable() throws BackendException {
		BoundStatement bs1 = new BoundStatement(DROP_TABLE_DIRECTOR);
		BoundStatement bs2 = new BoundStatement(CREATE_TABLE_DIRECTOR);

		try {
			session.execute(bs1);
			session.execute(bs2);
		} catch (Exception e) {
			throw new BackendException("Could not create table director. " + e.getMessage() + ".", e);
		}

		logger.info("Table director created");
	}

	public void createEmployeeTable() throws BackendException {
		BoundStatement bs1 = new BoundStatement(DROP_TABLE_EMPLOYEE);
		BoundStatement bs2 = new BoundStatement(CREATE_TABLE_EMPLOYEE);

		try {
			session.execute(bs1);
			session.execute(bs2);
		} catch (Exception e) {
			throw new BackendException("Could not create table employee. " + e.getMessage() + ".", e);
		}

		logger.info("Table employee created");
	}

	public void createSkillTable() throws BackendException {
		BoundStatement bs1 = new BoundStatement(DROP_TABLE_SKILL);
		BoundStatement bs2 = new BoundStatement(CREATE_TABLE_SKILL);

		try {
			session.execute(bs1);
			session.execute(bs2);
		} catch (Exception e) {
			throw new BackendException("Could not create table skill. " + e.getMessage() + ".", e);
		}

		logger.info("Table skill created");
	}

	public void createTaskTable() throws BackendException {
		BoundStatement bs1 = new BoundStatement(DROP_TABLE_TASK);
		BoundStatement bs2 = new BoundStatement(CREATE_TABLE_TASK);

		try {
			session.execute(bs1);
			session.execute(bs2);
		} catch (Exception e) {
			throw new BackendException("Could not create table task. " + e.getMessage() + ".", e);
		}

		logger.info("Table task created");
	}

	public void upsertDirector(String name) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_DIRECTOR);
		bs.bind(name);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Director " + name + " upserted");
	}

	public Director getDirector(String name) throws BackendException {
		List<Director> directors = new ArrayList<>();
		BoundStatement bs = new BoundStatement(SELECT_DIRECTOR);
		bs.bind(name);
	
		ResultSet rs;
		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
	
		for (Row row : rs) {
			List<String> tasks = row.getList("tasks", String.class);
			directors.add(new Director(name, tasks));
		}

		if (directors.isEmpty()) {
			return null;
		} else {
			return directors.get(0);
		}
	}

	public void addDirectorTask(String taskID, String directorName) throws BackendException {
		BoundStatement bs = new BoundStatement(ADD_DIRECTOR_TASK);
		bs.bind(taskID, directorName);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Task " + taskID + " added for director " + directorName);
	}

	public void removeDirectorTask(String taskID, String directorName) throws BackendException {
		BoundStatement bs = new BoundStatement(REMOVE_DIRECTOR_TASK);
		bs.bind(taskID, directorName);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Task " + taskID + " removed for director " + directorName);
	}

	public String upsertEmployee(String name, int age, List<String> skills) throws BackendException {
		String generatedUUID = UUID.randomUUID().toString();

		BoundStatement bs = new BoundStatement(INSERT_INTO_EMPLOYEE);
		bs.bind(generatedUUID, name, age, skills);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Employee " + name + " upserted");

		return generatedUUID;
	}
	
	public Employee getEmployee(String employeeID) throws BackendException {
		List<Employee> employees = new ArrayList<>();
		BoundStatement bs = new BoundStatement(SELECT_EMPLOYEE);
		bs.bind(employeeID);
	
		ResultSet rs;
		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
	
		for (Row row : rs) {
			String employeeId = row.getString("employee_id");
			String name = row.getString("name");
			int age = row.getInt("age");
			List<String> skills = row.getList("skills", String.class);
			String taskId = row.getString("task_id");
			employees.add(new Employee(employeeId, name, age, skills, taskId));
		}

		if (employees.isEmpty()) {
			return null;
		} else {
			return employees.get(0);
		}
	}

	public void updateEmployeeTask(String taskID, String employeeID) throws BackendException {
		BoundStatement bs = new BoundStatement(UPDATE_EMPLOYEE_TASK);
		bs.bind(taskID, employeeID);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Employee " + employeeID + " changed task to " + taskID);
	}

	public void upsertSkill(String skillName, String employeeID) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_SKILL);
		bs.bind(skillName, employeeID);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Skill " + skillName + " upserted");
	}
	
	public List<Skill> getSkills(String skillName) throws BackendException {
		List<Skill> skills = new ArrayList<>();
		BoundStatement bs = new BoundStatement(SELECT_SKILL);
		bs.bind(skillName);
	
		ResultSet rs;
		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
	
		for (Row row : rs) {
			String employeeId = row.getString("employee_id");
			skills.add(new Skill(skillName, employeeId));
		}
	
		return skills;
	}

	public String upsertTask(String name, String deadline, int peopleRequired, List<String> skillsRequired) throws BackendException {
		String generatedUUID = UUID.randomUUID().toString();

		BoundStatement bs = new BoundStatement(INSERT_INTO_DIRECTOR);
		bs.bind(generatedUUID, name, deadline, peopleRequired, skillsRequired);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Task " + name + " upserted");

		return generatedUUID;
	}

	public void addEmployeeToTask(String taskID, String employeeID) throws BackendException {
		BoundStatement bs = new BoundStatement(ADD_EMPLOYEE_TO_TASK);
		bs.bind(taskID, employeeID);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Task " + taskID + " upserted");
	}

	public Task getTask(String taskID) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_TASK);
		bs.bind(taskID);
	
		ResultSet rs;
		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		List<String> employeeIds = new ArrayList<>();
		String name = null;
		String deadline = null;
		Boolean finished = null;
		Integer peopleRequired = null;
		List<String> skillsRequired = null;

		for (Row row : rs) {
			String employeeId = row.getString("employee_id");
			if (employeeId != null) {
				employeeIds.add(employeeId);
			}
	
			// Pobierz dane taska, jeśli nie były pobrane, gdy są dostępne (czyli nie są `null`)
			if (name == null) {
				String potentialName = row.getString("name");
				String potentialDeadline = row.getString("deadline");
				Boolean potentialFinished = row.getBool("finished");
				Integer potentialPeopleRequired = row.getInt("people_required");
				List<String> potentialSkillsRequired = row.getList("skills_required", String.class);

				// Aktualizuj dane taska, jeśli wszystkie pola są dostępne
				if (potentialName != null && potentialDeadline != null && potentialFinished != null 
					&& potentialPeopleRequired != null && potentialSkillsRequired != null) {
					name = potentialName;
					deadline = potentialDeadline;
					finished = potentialFinished;
					peopleRequired = potentialPeopleRequired;
					skillsRequired = potentialSkillsRequired;
				}
			}
		}

		Task task = new Task(taskID, employeeIds, name, deadline, finished, peopleRequired, skillsRequired);
		return task;
	}

	public void finishTask(String taskID) throws BackendException {
		BoundStatement bs = new BoundStatement(FINISH_TASK);
		bs.bind(taskID);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Task " + taskID + " finished");
	}

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}
