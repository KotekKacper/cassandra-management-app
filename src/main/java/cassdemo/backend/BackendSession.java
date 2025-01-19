package cassdemo.backend;

import cassdemo.types.Director;
import cassdemo.types.Employee;
import cassdemo.types.Skill;
import cassdemo.types.Task;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.UUID;


public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	private Session session;

	private static PreparedStatement CREATE_CLEAR_DIRECTOR;
	private static PreparedStatement CREATE_CLEAR_EMPLOYEE;
	private static PreparedStatement CREATE_CLEAR_SKILL;
	private static PreparedStatement CREATE_CLEAR_TASK;
	private static PreparedStatement INSERT_INTO_DIRECTOR;
	private static PreparedStatement SELECT_DIRECTOR;
	private static PreparedStatement UPDATE_DIRECTOR_TASKS;
	private static PreparedStatement INSERT_INTO_EMPLOYEE;
	private static PreparedStatement SELECT_EMPLOYEE;
	private static PreparedStatement UPDATE_EMPLOYEE;
	private static PreparedStatement INSERT_INTO_SKILL;
	private static PreparedStatement SELECT_SKILL;
	private static PreparedStatement INSERT_TASK;
	private static PreparedStatement SELECT_TASK;
	private static PreparedStatement FINISH_TASK;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private void prepareStatements() throws BackendException {
		try {
			// SELECT_COLUMNS = session.prepare("SELECT * FROM dupa;").setConsistencyLevel(ConsistencyLevel.ONE);

			CREATE_CLEAR_DIRECTOR = session.prepare("DROP TABLE IF EXISTS Director;" +
													"CREATE TABLE Director (name text, tasks list<text>, PRIMARY KEY (name));");
			CREATE_CLEAR_EMPLOYEE = session.prepare("DROP TABLE IF EXISTS Employee;" +
													"CREATE TABLE Employee (employee_id text, name text, age int, skills list<text>, task_id text, PRIMARY KEY (employee_id));");
			CREATE_CLEAR_SKILL = session.prepare("DROP TABLE IF EXISTS Skill;" +
												 "CREATE TABLE Skill (skill_name text, employee_id text, PRIMARY KEY (skill_name), employee_id);");
			CREATE_CLEAR_TASK = session.prepare("DROP TABLE IF EXISTS Task;" +
												"CREATE TABLE Task (task_id text, employee_id text, name text, deadline text, finished boolean, people_required int, skills_required list<text>, PRIMARY KEY ((task_id), employee_id));");
			INSERT_INTO_DIRECTOR = session.prepare("INSERT INTO Director (name) VALUES (?);");
			SELECT_DIRECTOR = session.prepare("SELECT * FROM Director WHERE name = ?;");
			ADD_DIRECTOR_TASK = session.prepare("UPDATE Director SET tasks = tasks + ? WHERE name = ?;");
			REMOVE_DIRECTOR_TASK = session.prepare("UPDATE Director SET tasks = tasks - ? WHERE name = ?;");
			INSERT_INTO_EMPLOYEE = session.prepare("INSERT INTO Employee (employee_id, name, age, skills) VALUES (?, ?, ?, ?);");
			SELECT_EMPLOYEE = session.prepare("SELECT * FROM Director WHERE name = ?;");
			UPDATE_EMPLOYEE_TASK = session.prepare("UPDATE Employee SET task_id = ? WHERE employee_id = ?;");
			INSERT_INTO_SKILL = session.prepare("INSERT INTO Skill (skill_name, employee_id) VALUES (?, ?);");
			SELECT_SKILL = session.prepare("SELECT * FROM Director WHERE name = ?;");
			INSERT_TASK = session.prepare("INSERT INTO Task (task_id, name, deadline, people_required, skills_required, employee_id) VALUES (?, ?, ?, ?, ?);");
			ADD_EMPLOYEE_TO_TASK = session.prepare("INSERT INTO Task (task_id, employee_id) VALUES (?, ?);");
			SELECT_TASK = session.prepare("SELECT * FROM Task WHERE task_id = ?;");
			FINISH_TASK = session.prepare("UPDATE Task SET finished = true WHERE task_id = ?;");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public void createDirectorTable() throws BackendException {
		BoundStatement bs = new BoundStatement(CREATE_CLEAR_DIRECTOR);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not create table director. " + e.getMessage() + ".", e);
		}

		logger.info("Table director created");
	}

	public void createEmployeeTable() throws BackendException {
		BoundStatement bs = new BoundStatement(CREATE_CLEAR_EMPLOYEE);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not create table employee. " + e.getMessage() + ".", e);
		}

		logger.info("Table employee created");
	}

	public void createSkillTable() throws BackendException {
		BoundStatement bs = new BoundStatement(CREATE_CLEAR_SKILL);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not create table skill. " + e.getMessage() + ".", e);
		}

		logger.info("Table skill created");
	}

	public void createTaskTable() throws BackendException {
		BoundStatement bs = new BoundStatement(CREATE_CLEAR_TASK);

		try {
			session.execute(bs);
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

	public List<Director> getDirectors(String name) throws BackendException {
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
			String name = row.getString("name");
			List<String> tasks = row.getList("tasks", String.class);
			directors.add(new Director(name, tasks));
		}
	
		return directors;
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

		BoundStatement bs = new BoundStatement(INSERT_INTO_DIRECTOR);
		bs.bind(generatedUUID, name, skills);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Employee " + name + " upserted");

		return generatedUUID;
	}
	
	public List<Employee> getEmployees(String employeeID) throws BackendException {
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
	
		return employees;
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
	
	public List<Skill> selectSkills(String skillName) throws BackendException {
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
			String skillName = row.getString("skill_name");
			String employeeId = row.getString("employee_id");
			skills.add(new Skill(skillName, employeeId));
		}
	
		return skills;
	}

	public String upsertTask(String name, String deadline, int peopleRequired, List<String> skillsRequired) throws BackendException {
		String generatedUUID = UUID.randomUUID().toString();

		BoundStatement bs = new BoundStatement(INSERT_INTO_DIRECTOR);
		bs.bind(generatedUUID, name, deadline, people_required, skillsRequired);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Task " + name + " upserted");

		return generatedUUID;
	}

	public void upsertTask(String taskID, String employeeID) throws BackendException {
		BoundStatement bs = new BoundStatement(ADD_EMPLOYEE_TO_TASK);
		bs.bind(taskID, employeeID);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("Task " + name + " upserted");
	}

	public List<Task> selectTasks(String taskID) throws BackendException {
		List<Task> tasks = new ArrayList<>();
		BoundStatement bs = new BoundStatement(SELECT_TASK);
		bs.bind(taskID);
	
		ResultSet rs;
		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
	
		for (Row row : rs) {
			String taskId = row.getString("task_id");
			String employeeId = row.getString("employee_id");
			String name = row.getString("name");
			String deadline = row.getString("deadline");
			Boolean finished = row.getBool("finished");
			Integer peopleRequired = row.getInt("people_required");
			List<String> skillsRequired = row.getList("skills_required", String.class);
			tasks.add(new Task(taskId, employeeId, name, deadline, finished, peopleRequired, skillsRequired));
		}
	
		return tasks;
	}
	
	private static PreparedStatement FINISH_TASK;




	public String selectAll() throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_USERS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			String rcompanyName = row.getString("companyName");
			String rname = row.getString("name");
			int rphone = row.getInt("phone");
			String rstreet = row.getString("street");

			builder.append(String.format(USER_FORMAT, rcompanyName, rname, rphone, rstreet));
		}

		return builder.toString();
	}

	public void upsertUser(String companyName, String name, int phone, String street) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_USERS);
		bs.bind(companyName, name, phone, street);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("User " + name + " upserted");
	}

	public void deleteAll() throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FROM_USERS);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
		}

		logger.info("All users deleted");
	}

	public boolean isNickFree(String nick) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_NICKS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			String rname = row.getString("name");
			boolean ravailable = row.getBool("avalible");
			int rpid = row.getInt("pid");

			if (Objects.equals(rname, nick)) {
                return ravailable;
			}
		}
		return true;
	}

	public void insertNick(String nick, int pid) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_NICKS);
		bs.bind(nick, false, pid);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

//		logger.info("User " + nick + " upserted");
	}

	public boolean isNickOur(String nick, int pid) throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_NICKS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			String rname = row.getString("name");
			boolean ravailable = row.getBool("avalible");
			int rpid = row.getInt("pid");

			if (Objects.equals(rname, nick)) {
				if (rpid == pid) {
//					System.out.println("Our nick: " + nick);
					return true;
				}
//				System.out.println("Nick: " + nick + " of " + rpid);
				logger.info("Nick: " + nick + " of " + rpid);
			}
		}
		return false;
	}

	public void deleteNick(String nick) throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_NICK);
		bs.bind(nick);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a deletion. " + e.getMessage() + ".", e);
		}

//		logger.info("Deleted " + nick);
	}

	public void setNick(int pid) throws BackendException, InterruptedException {
		while (true) {
			// 1.
			String chosenNick = getRandom(nicks);
//			System.out.println("1. Chosen Nick: " + chosenNick);
			// 2.
			boolean nickFree = isNickFree(chosenNick);
//			System.out.println("2. Nick Free: " + nickFree);
			// 3.
			if (!nickFree) {
				continue;
			}
			// 4.
			insertNick(chosenNick, pid);
//			System.out.println("4. Nick inserted");
			// 5. i 6.
			boolean nickOur = isNickOur(chosenNick, pid);
			if (!nickOur) {
				continue;
			}
//			System.out.println("5. Is Nick our:" + nickOur);
			// 7.
			TimeUnit.MILLISECONDS.sleep(1);
			// 8.
			deleteNick(chosenNick);
//			System.out.println("8. Deleted nick");
		}
	}

	public void updateColumns(int pid) throws BackendException {
		BoundStatement bs = new BoundStatement(UPDATE_COLUMNS);
		bs.bind(pid, -1*pid);
//		logger.info("Set value: "+pid);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}
	}

	public void selectColumns() throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_COLUMNS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			int id = row.getInt("id");
			int col1 = row.getInt("col1");
			int col2 = row.getInt("col2");
			if (col1 != -col2) {
				logger.info("ID: "+id+" Col1: "+col1+" Col2: "+col2);
			}
		}
	}

	public void zad4(int pid) throws BackendException, InterruptedException {
		while(true) {
			updateColumns(pid);
			selectColumns();
			TimeUnit.MILLISECONDS.sleep(1);
		}
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
