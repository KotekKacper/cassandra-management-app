package cassdemo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ConsistencyLevel;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// CREATE TABLE IF NOT EXISTS nicks (nick text,id text,PRIMARY KEY (nick));

public class NewThread extends Thread  {
	// public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
	// public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ANY;
	public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ALL;

	private int threadNumber;
	private String contactPoint;
	private String keyspace;

	public NewThread(int threadNumber, String contactPoint, String keyspace) {
		this.threadNumber = threadNumber;
		this.contactPoint = contactPoint;
		this.keyspace = keyspace;
	}

	public void run() {

		List<String> elements = new ArrayList<>();
		elements.add("P");
		elements.add("B");
		elements.add("N");
		elements.add("M");
		
		Random random = new Random();
        	
		String id = "P_" + String.valueOf(this.threadNumber);
		
		Cluster cluster = Cluster.builder().addContactPoint(this.contactPoint).build();
		Session session = cluster.connect(this.keyspace);
		
		PreparedStatement SELECT_USER = session.prepare("SELECT * FROM nicks WHERE nick=?;").setConsistencyLevel(CONSISTENCY_LEVEL);
		PreparedStatement INSERT_USER = session.prepare("INSERT INTO nicks (nick, id) VALUES (?, ?);").setConsistencyLevel(CONSISTENCY_LEVEL);
		PreparedStatement DELETE_USER = session.prepare("DELETE FROM nicks WHERE nick=?;").setConsistencyLevel(CONSISTENCY_LEVEL);
		
		String user_id;
		for (int i = 0; i < 10; i++) {
			String identifier = String.valueOf(this.threadNumber) + ":" + String.valueOf(i);
			int randomIndex = random.nextInt(elements.size());
			String name = elements.get(randomIndex);
			
			System.out.println("\t" + identifier + " Name " + name); 
			user_id = get_user_id(session, SELECT_USER, name);
			if(!user_id.equals("")) {
				continue;
			}

			add_user(session, INSERT_USER, name, id);

			user_id = get_user_id(session, SELECT_USER, name);
			if(!user_id.equals(id)) {
				System.out.println("\t\t" + identifier + " Konflikt: wątek:" + String.valueOf(this.threadNumber) + " Pętla: " + String.valueOf(i) + " nick: " + name + " Jest: " + user_id + " Powinno być: " + id); 
				continue;
			}
			
			try {
			    Thread.sleep(1);
			} catch (InterruptedException e) {
			    e.printStackTrace();
			}
			del_user(session, DELETE_USER, name);
		}
	}

    private static String get_user_id(Session session, PreparedStatement x, String name) {
    	BoundStatement bs = new BoundStatement(x);
		bs.bind(name);
		ResultSet rs = session.execute(bs);
		for (Row row : rs) {
			return row.getString("id");
		}
		return "";
    }
    
    
    private static void add_user(Session session, PreparedStatement x, String name, String id) {
    	BoundStatement bs = new BoundStatement(x);
		bs.bind(name, id);
		session.execute(bs);
    }
    
    private static void del_user(Session session, PreparedStatement x, String name) {
    	BoundStatement bs = new BoundStatement(x);
		bs.bind(name);
		session.execute(bs);
    }
}



