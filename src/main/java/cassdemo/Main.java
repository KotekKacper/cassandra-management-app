package cassdemo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/*
 * docker-compose up -d
 * docker exec -it cassandra1 bash
 * cqlsh 127.0.0.1 9042
 * 
 * DESCRIBE keyspaces; 		--lista keyspace
 * CREATE KEYSPACE IF NOT EXISTS EmployeeManagement WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }; --tworzenie keyspace
 * USE EmployeeManagement;
 * 
 * gradle run -- uruchomienie programu
 */

public class Main {
	private static final String PROPERTIES_FILENAME = "config.properties";


	public static void main(String[] args) {
		String contactPoint = null;
		String keyspace = null;

		System.out.println("HERE1");

		Properties properties = new Properties();
		try {
			properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

			contactPoint = properties.getProperty("contact_point");
			keyspace = properties.getProperty("keyspace");
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		System.out.println("HERE2");

		Operations op = new Operations(contactPoint, keyspace);
		op.initDatabase();
		op.addEmployee("Patryk Lukaszewski", 100, Arrays.asList("programming", "dancing"));
		op.addEmployee("Kacper Garncarek", 200, Arrays.asList("programming", "music"));

		System.exit(0);
	}
}
