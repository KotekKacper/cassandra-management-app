package cassdemo.backend;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private final String[] nicks = {"Kacper", "Michał", "Jan", "Konrad"};

	public static String getRandom(String[] array) {
		int rnd = new Random().nextInt(array.length);
		return array[rnd];
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

	private static PreparedStatement SELECT_ALL_FROM_USERS;
	private static PreparedStatement INSERT_INTO_USERS;
	private static PreparedStatement DELETE_ALL_FROM_USERS;
	private static PreparedStatement SELECT_ALL_FROM_NICKS;
	private static PreparedStatement INSERT_INTO_NICKS;
	private static PreparedStatement DELETE_NICK;
	private static PreparedStatement UPDATE_COLUMNS;
	private static PreparedStatement SELECT_COLUMNS;

	private static final String USER_FORMAT = "- %-10s  %-16s %-10s %-10s\n";
	// private static final SimpleDateFormat df = new
	// SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private void prepareStatements() throws BackendException {
		try {
			SELECT_ALL_FROM_USERS = session.prepare("SELECT * FROM users;");
			INSERT_INTO_USERS = session
					.prepare("INSERT INTO users (companyName, name, phone, street) VALUES (?, ?, ?, ?);");
			DELETE_ALL_FROM_USERS = session.prepare("TRUNCATE users;");

			SELECT_ALL_FROM_NICKS = session.prepare("SELECT * FROM nicks;").setConsistencyLevel(ConsistencyLevel.QUORUM);
			INSERT_INTO_NICKS = session.prepare("INSERT INTO nicks (name, avalible, pid) VALUES (?, ?, ?);").setConsistencyLevel(ConsistencyLevel.QUORUM);
			DELETE_NICK = session.prepare("DELETE FROM nicks WHERE name = ?;").setConsistencyLevel(ConsistencyLevel.QUORUM);
			UPDATE_COLUMNS = session.prepare("INSERT INTO dupa (id, col1, col2) VALUES (0, ?, ?);").setConsistencyLevel(ConsistencyLevel.ONE);
			SELECT_COLUMNS = session.prepare("SELECT * FROM dupa;").setConsistencyLevel(ConsistencyLevel.ONE);
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

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
