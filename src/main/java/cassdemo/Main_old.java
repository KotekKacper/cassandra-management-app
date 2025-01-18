package cassdemo;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;

public class Main_old {

	private static final String PROPERTIES_FILENAME = "config.properties";

	public static void main(String[] args) throws IOException, BackendException, InterruptedException {
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
			
		BackendSession session = new BackendSession(contactPoint, keyspace);

//		session.upsertUser("PP", "Adam", 609, "A St");
//		session.upsertUser("PP", "Ola", 509, null);
//		session.upsertUser("UAM", "Ewa", 720, "B St");
//		session.upsertUser("PP", "Kasia", 713, "C St");

		final int threadCount = 30;
		Thread[] threads = new Thread[threadCount];

		for (int i = 0; i < threadCount; i++) {
			int finalI = i;
			threads[i] = new Thread(() -> {
				try {
//					session.setNick(10000 + finalI);
					session.zad4(1000+finalI);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			threads[i].start();
		}

		for (int i = 0; i < threadCount; i++) {
			threads[i].join();
		}

//		session.setNick();
//		System.out.println("Users: \n" + output);

//		session.deleteAll();

		System.exit(0);
	}
}
