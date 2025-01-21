package cassdemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Collections;
import java.util.Random;



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

		test(contactPoint, keyspace);

		System.exit(0);
	}

	private static void test(String contactPoint, String keyspace) {
		int NUMBER_THREADS = 5;
		int NUMBER_TASK_PER_THREAD = 50;
		int NUMBER_EMPLOYEE = 200;
		int NUMBER_DIRECTOR = 2;
		int MIN_REQUIRE_EMPLOYEE = 5;
		int MAX_REQUIRE_EMPLOYEE = 5;

		List<String> listDirectors = new ArrayList<>();
		List<String> listSkills = Arrays.asList("programming", "dancing", "musics", "writer");
		Operations op = new Operations(contactPoint, keyspace);
		op.initDatabase();
		Random random = new Random();
		for(int i=0; i<NUMBER_EMPLOYEE; i++) {
			List<String> result = new ArrayList<>();

			int numSkillsFromList = random.nextInt(listSkills.size()) + 1;
			Collections.shuffle(listSkills);
			for (int j = 0; j < numSkillsFromList; j++) {
				result.add(listSkills.get(j));
			}
			op.addEmployee("E_"+String.valueOf(i), i, result);
		}
		for(int i=0; i<NUMBER_DIRECTOR; i++) {
			String directorName = "D_" + String.valueOf(i);
			op.addDirector(directorName);
			listDirectors.add(directorName);
		}

		NewThread[] listOfThreads = new NewThread[NUMBER_THREADS];
		for(int i=0; i<NUMBER_THREADS; i++) {
			NewThread t = new NewThread(i, op, listDirectors, NUMBER_TASK_PER_THREAD, listSkills, MIN_REQUIRE_EMPLOYEE, MAX_REQUIRE_EMPLOYEE);
			listOfThreads[i] = t;
			listOfThreads[i].start();
		}

		for(int i=0; i<NUMBER_THREADS; i++) {
			try {
				listOfThreads[i].join();
			}
			catch(InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		for(int i=0; i<12; i++) {
			System.out.println(String.valueOf(i+1) + ". " + op.descStats[i] + " = " + String.valueOf(op.listStats[i]));
		}
	}
}
