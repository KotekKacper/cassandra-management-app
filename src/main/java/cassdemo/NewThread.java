package cassdemo;

import com.datastax.driver.core.ConsistencyLevel;

import java.util.*;

public class NewThread extends Thread  {
	 public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
	// public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ANY;
//	public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ALL;

	private final int threadNumber;
	private final Operations op;
	private final List<String> listDirectors;
	private final List<String> listSkills;
	private final int number_task;
	private final int min_employee_in_task;
	private final int max_employee_in_task;

	public NewThread(int threadNumber, Operations op, List<String> listDirectors, int number_task, List<String> listSkills, int min_employee_in_task, int max_employee_in_task) {
		this.threadNumber = threadNumber;
		this.op = op;
		this.listDirectors = listDirectors;
		this.listSkills = listSkills;
		this.number_task = number_task;
		this.min_employee_in_task = min_employee_in_task;
		this.max_employee_in_task = max_employee_in_task;
	}

	public void run() {
		Random random = new Random();
		String id = "P_" + String.valueOf(this.threadNumber);

		int numberDirectors = listDirectors.size();
		for(int i=0; i<number_task; i++) {
			List<String> result = new ArrayList<>();
			int numSkillsFromList = random.nextInt(listSkills.size()) + 1;
			Collections.shuffle(listSkills);
			for (int j = 0; j < numSkillsFromList; j++) {
				result.add(listSkills.get(j));
			}

			int requireEmployee = random.nextInt(max_employee_in_task - min_employee_in_task + 1) + min_employee_in_task;

			String taskName = id+"_T_" + String.valueOf(i);
			int directorNr = random.nextInt(numberDirectors);
			String taskID = op.addTaskAndAssignTaskToEmployees(listDirectors.get(directorNr), taskName, "01.01.2026", requireEmployee, result);
			if(taskID == null) continue;
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			op.finishTask(listDirectors.get(0), taskID);
		}
	}
}


