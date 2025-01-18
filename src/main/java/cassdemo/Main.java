package cassdemo;

import java.io.IOException;
import java.util.Properties;


/*
 * docker-compose up -d
 * docker exec -it cassandra1 bash
 * cqlsh 127.0.0.1 9042
 * 
 * DESCRIBE keyspaces; 		--lista keyspace
 * CREATE KEYSPACE IF NOT EXISTS Test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }; --tworzenie keyspace
 * USE test;
 * 
 * CREATE TABLE IF NOT EXISTS nicks (nick text, id text, PRIMARY KEY (nick)); --tabela używana w NewThread
 * 
 * gradle run -- uruchomienie programu
 */


public class Main {
	private static final String PROPERTIES_FILENAME = "config.properties";
	private static final int NUMBER_THREADS = 5;


	public static void main(String[] args) {
		String contactPoint = null;
		String keyspace = null;

		Properties properties = new Properties();
		try {
			properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

			contactPoint = properties.getProperty("contact_point");
			keyspace = properties.getProperty("keyspace");
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		NewThread[] listOfThreads = new NewThread[NUMBER_THREADS];
		for(int i=0; i<NUMBER_THREADS; i++) {
			NewThread t = new NewThread(i, contactPoint, keyspace);
			listOfThreads[i] = t;
			listOfThreads[i].start();
		}
		for(int i=0; i<NUMBER_THREADS; i++) {
			try {
				listOfThreads[i].join();
			}
			catch(Exception e) {}
		}
		System.exit(0);
	}
}





