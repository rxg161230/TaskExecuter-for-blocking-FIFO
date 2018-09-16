package edu.utdallas.taskExecutorImpl;

import java.util.List;
import java.util.ArrayList;

import edu.utdallas.blockingFIFO.BlockingQueue;
import edu.utdallas.taskExecutor.Task;
import edu.utdallas.taskExecutor.TaskExecutor;

/** A task executor that concurrently executes any number of {@link edu.utdallas.taskExecutor.Task}.  Uses an internal
 * pool of non-terminating worker threads in order to avoid the overhead of spawning a new thread for every task.  The
 * number of threads is bounded in order to avoid thrashing and/or out-of-memory errors.  Research the "worker threads"
 * and "thread pool" design patterns for more information.
 *  
 * <p>
 * The number of worker threads is fixed and set at initialization time.  All worker threads are created and started
 * at initialization time, which may result in a noticeable delay at first if the number of threads is very large.
 * However, this eager initialization strategy means that the TaskExecutorImpl will be ready to execute all tasks
 * efficiently (with minimal overhead) immediately following initialization.
 * 
 * <p>
 * It is both inefficient and unnecessary to set the size of the thread pool larger than the number of tasks to execute.
 * Obversely, the size of the thread pool should be set as large as possible--without resulting in thrashing--to see
 * optimal throughput.  However, throughput as a function of the number of threads does experience diminishing marginal
 * returns even without thrashing.  In particular, the number of processors in a system and the I/O demands of the tasks
 * should be taken into account.
 * 
 * <p>
 * If the aggregate rate of producing tasks (adding tasks to this TaskExecutorImpl) is faster than the aggregate rate of
 * consuming tasks (execution of the tasks by the pool or worker threads), then {@link #addTask(Task)} may temporarily 
 * block.
 * 
 * <p>
 * Tasks are executed in approximate FIFO order, with some variation to be expected due to thread scheduling dynamics at
 * the system level.
 * 
 * <p>
 * Any errors or exceptions that arise in the course of executing the client code of a <code>Task</code> will be logged
 * to this process' <code>System.err</code> along with the task's name according to
 * {@link edu.utdallas.taskExecutor.Task#getName()}.  The executing worker thread will then abandon the offending
 * <code>Task</code> and proceed to executing the next <code>Task</code>, if any.
 * 
 * @version 1.0.0, 2016-10-24
 * @since 1.0.0
 */
public class TaskExecutorImpl implements TaskExecutor
{
	private static final int QUEUE_SIZE = 100;  // default bound for taskQueue
	
	private List<Thread> runnerPool;  // a pool of runner threads that will execute tasks from taskQueue
	private BlockingQueue<Task> taskQueue;  // a queue of tasks that threads from runnerPool will execute
	
	/** Creates a new <code>TaskExecutorImpl</code> with a number of worker threads (pool size) equal to the
	 * <code>numRunners</code> argument.  The worker threads are all created and started at initialization time.  All
	 * worker threads will immediately become blocked until such time that tasks are added to this
	 * <code>TaskExecutorImpl</code>.
	 * 
	 * <p>
	 * Does not validate the "numRunners" argument. If negative, will result in an exception. If zero, no task will ever
	 * be executed.
	 * 
	 * @param numRunners the size of the worker thread pool
	 * @throws IllegalArgumentException if "numRunners" argument is negative
	 */
	public TaskExecutorImpl(int numRunners)
	{
		this.runnerPool = new ArrayList<>(numRunners);
		this.taskQueue = new BlockingQueue<>(QUEUE_SIZE);
		
		// Populate runnerPool with started task runner threads.
		Thread taskRunner;
		for (int i = 0; i < numRunners; ++i) {
			
			taskRunner = new Thread( () -> {
				Task task;
				while (true) {
					task = taskQueue.take();
					try {
						task.execute();
					} catch (Throwable th) {
						try { System.err.println(task.getName() + ":"); } catch (Throwable th2) { /* no-op */ }  // if task == null
						System.err.println(th.getMessage());
						System.err.println(th.getStackTrace());
						System.err.println();
					}
				}
			});
			
			taskRunner.setName("TaskThread" + runnerPool.size());
			
			taskRunner.start();
			runnerPool.add(taskRunner);
		}
	}
	
	/** Adds a {@link edu.utdallas.taskExecutor.Task} to this <code>TaskExecutorImpl</code>.  Tasks will be executed in
	 * approximate FIFO order.
	 * 
	 * <p>
	 * This method may block if the aggregate rate of producing tasks (adding tasks to this TaskExecutorImpl) is faster
	 * than the aggregate rate of consuming tasks (execution of the tasks by the pool or worker threads).
	 * 
	 * <p>
	 * Does not validate the <code>task</code> argument.  A <code>null</code> argument will not cause an exception in 
	 * the client code invoking this method, but will cause the worker thread that attempts to execute the null tak to
	 * log a <code>NullPointerException</code> to this process' <code>System.err</code>.
	 * 
	 * @param task the {@link edu.utdallas.taskExecutor.Task} to be added for execution
	 */
	@Override
	public void addTask(Task task)
	{
		taskQueue.put(task);
	}
}
