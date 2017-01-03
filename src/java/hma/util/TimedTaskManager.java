/**
 *
 */
package hma.util;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author guoyezhi
 */
public class TimedTaskManager {

    private ExecutorService exec = null;

    /**
     * The timed task queue. This data structure is shared with the scheduler
     * thread. This manager produces tasks, via its various schedule calls, and the
     * scheduler thread consumes, executing timer tasks as appropriate, and removing
     * them from the queue when they're obsolete.
     */
    private TimedTaskQueue queue = new TimedTaskQueue();

    /**
     * The timer thread.
     */
    private SchedulerThread thread = new SchedulerThread(queue);

    /**
     * This object causes the manager's task execution thread to exit gracefully
     * when there are no live references to the TimedTaskManager object and no
     * tasks in the timed queue. It is used in preference to a finalizer on
     * TimedTaskManager as such a finalizer would be susceptible to a subclass's
     * finalizer forgetting to call it.
     */
    @SuppressWarnings("unused")
    private Object threadReaper = new Object() {
        protected void finalize() throws Throwable {
            synchronized (queue) {
                thread.newTasksMayBeScheduled = false;
                queue.notify(); // In case queue is empty.
            }
        }
    };


    /**
     * This ID is used to generate thread names.
     */
    private static AtomicInteger nextSerialNumber = new AtomicInteger(0);

    private static synchronized int serialNumber() {
        return nextSerialNumber.addAndGet(1);
    }

    /**
     * Creates a new timed task manager.
     *
     * @see Thread
     * @see #cancel()
     */
    public TimedTaskManager() {
        this("TimedTaskManager-" + serialNumber(), 128);
    }

    /**
     * Creates a new timed task manager whose associated thread has the
     * specified name.
     *
     * @param name the name of the associated thread
     * @throws NullPointerException if name is null
     * @see Thread#getName()
     */
    public TimedTaskManager(String name) {
        this(name, 128);
    }

    /**
     * Creates a new timed task manager.
     *
     * @param nThreads the number of threads in the TimedTaskManager's timed
     *                 task thread pool
     * @throws NullPointerException if name is null
     * @see Thread#getName()
     */
    public TimedTaskManager(int nThreads) {
        this("TimedTaskManager-" + serialNumber(), nThreads);
    }

    /**
     * Creates a new timed task manager whose associated thread has the
     * specified name.
     *
     * @param name     the name of the associated thread
     * @param nThreads the number of threads in the TimedTaskManager's timed
     *                 task thread pool
     * @throws NullPointerException if name is null
     * @see Thread#getName()
     */
    public TimedTaskManager(String name, int nThreads) {
        exec = Executors.newFixedThreadPool(nThreads);
        thread.setName(name);
        thread.start();
    }


    /**
     * Schedules the specified task for execution after the specified delay.
     *
     * @param task  task to be scheduled.
     * @param delay delay in milliseconds before task is to be executed.
     * @throws IllegalArgumentException if <tt>delay</tt> is negative, or
     *                                  <tt>delay + System.currentTimeMillis()</tt> is negative.
     * @throws IllegalStateException    if task was already scheduled or cancelled, or timer was
     *                                  cancelled.
     */
    public void schedule(TimedTask task, long delay) {
        if (delay < 0)
            throw new IllegalArgumentException("Negative delay.");
        sched(task, System.currentTimeMillis() + delay, 0);
    }

    /**
     * Schedules the specified task for execution at the specified time. If the
     * time is in the past, the task is scheduled for immediate execution.
     *
     * @param task task to be scheduled.
     * @param time time at which task is to be executed.
     * @throws IllegalArgumentException if <tt>time.getTime()</tt> is negative.
     * @throws IllegalStateException    if task was already scheduled or cancelled, timer was
     *                                  cancelled, or timer thread terminated.
     */
    public void schedule(TimedTask task, Date time) {
        sched(task, time.getTime(), 0);
    }

    /**
     * Schedules the specified task for repeated <i>fixed-delay execution</i>,
     * beginning after the specified delay. Subsequent executions take place at
     * approximately regular intervals separated by the specified period.
     * <p/>
     * <p/>
     * In fixed-delay execution, each execution is scheduled relative to the
     * actual execution time of the previous execution. If an execution is
     * delayed for any reason (such as garbage collection or other background
     * activity), subsequent executions will be delayed as well. In the long
     * run, the frequency of execution will generally be slightly lower than the
     * reciprocal of the specified period (assuming the system clock underlying
     * <tt>Object.wait(long)</tt> is accurate).
     *
     * @param task   task to be scheduled.
     * @param delay  delay in milliseconds before task is to be executed.
     * @param period time in milliseconds between successive task executions.
     * @throws IllegalArgumentException if <tt>delay</tt> is negative, or
     *                                  <tt>delay + System.currentTimeMillis()</tt> is negative.
     * @throws IllegalStateException    if task was already scheduled or cancelled, timer was
     *                                  cancelled, or timer thread terminated.
     */
    public void schedule(TimedTask task, long delay, long period) {
        if (delay < 0)
            throw new IllegalArgumentException("Negative delay.");
        if (period <= 0)
            throw new IllegalArgumentException("Non-positive period.");
        sched(task, System.currentTimeMillis() + delay, -period);
    }

    /**
     * Schedules the specified task for repeated <i>fixed-delay execution</i>,
     * beginning at the specified time. Subsequent executions take place at
     * approximately regular intervals, separated by the specified period.
     * <p/>
     * <p/>
     * In fixed-delay execution, each execution is scheduled relative to the
     * actual execution time of the previous execution. If an execution is
     * delayed for any reason (such as garbage collection or other background
     * activity), subsequent executions will be delayed as well. In the long
     * run, the frequency of execution will generally be slightly lower than the
     * reciprocal of the specified period (assuming the system clock underlying
     * <tt>Object.wait(long)</tt> is accurate).
     *
     * @param task      task to be scheduled.
     * @param firstTime First time at which task is to be executed.
     * @param period    time in milliseconds between successive task executions.
     * @throws IllegalArgumentException if <tt>time.getTime()</tt> is negative.
     * @throws IllegalStateException    if task was already scheduled or cancelled, timer was
     *                                  cancelled, or timer thread terminated.
     */
    public void schedule(TimedTask task, Date firstTime, long period) {
        if (period <= 0)
            throw new IllegalArgumentException("Non-positive period.");
        sched(task, firstTime.getTime(), -period);
    }

    /**
     * Schedules the specified task for repeated <i>fixed-rate execution</i>,
     * beginning after the specified delay. Subsequent executions take place at
     * approximately regular intervals, separated by the specified period.
     * <p/>
     * <p/>
     * In fixed-rate execution, each execution is scheduled relative to the
     * scheduled execution time of the initial execution. If an execution is
     * delayed for any reason (such as garbage collection or other background
     * activity), two or more executions will occur in rapid succession to
     * "catch up." In the long run, the frequency of execution will be exactly
     * the reciprocal of the specified period (assuming the system clock
     * underlying <tt>Object.wait(long)</tt> is accurate).
     *
     * @param task   task to be scheduled.
     * @param delay  delay in milliseconds before task is to be executed.
     * @param period time in milliseconds between successive task executions.
     * @throws IllegalArgumentException if <tt>delay</tt> is negative, or
     *                                  <tt>delay + System.currentTimeMillis()</tt> is negative.
     * @throws IllegalStateException    if task was already scheduled or cancelled, timer was
     *                                  cancelled, or timer thread terminated.
     */
    public void scheduleAtFixedRate(TimedTask task, long delay, long period) {
        if (delay < 0)
            throw new IllegalArgumentException("Negative delay.");
        if (period <= 0)
            throw new IllegalArgumentException("Non-positive period.");
        sched(task, System.currentTimeMillis() + delay, period);
    }

    /**
     * Schedules the specified task for repeated <i>fixed-rate execution</i>,
     * beginning at the specified time. Subsequent executions take place at
     * approximately regular intervals, separated by the specified period.
     * <p/>
     * <p/>
     * In fixed-rate execution, each execution is scheduled relative to the
     * scheduled execution time of the initial execution. If an execution is
     * delayed for any reason (such as garbage collection or other background
     * activity), two or more executions will occur in rapid succession to
     * "catch up." In the long run, the frequency of execution will be exactly
     * the reciprocal of the specified period (assuming the system clock
     * underlying <tt>Object.wait(long)</tt> is accurate).
     *
     * @param task      task to be scheduled.
     * @param firstTime First time at which task is to be executed.
     * @param period    time in milliseconds between successive task executions.
     * @throws IllegalArgumentException if <tt>time.getTime()</tt> is negative.
     * @throws IllegalStateException    if task was already scheduled or cancelled, timer was
     *                                  cancelled, or timer thread terminated.
     */
    public void scheduleAtFixedRate(TimedTask task, Date firstTime, long period) {
        if (period <= 0)
            throw new IllegalArgumentException("Non-positive period.");
        sched(task, firstTime.getTime(), period);
    }

    /**
     * Schedule the specified timed task for execution at the specified time
     * with the specified period, in milliseconds. If period is positive, the
     * task is scheduled for repeated execution; if period is zero, the task is
     * scheduled for one-time execution. Time is specified in Date.getTime()
     * format. This method checks timer state, task state, and initial execution
     * time, but not period.
     *
     * @throws IllegalArgumentException if <tt>time()</tt> is negative.
     * @throws IllegalStateException    if task was already scheduled or cancelled, timer was
     *                                  cancelled, or timer thread terminated.
     */
    private void sched(TimedTask task, long time, long period) {
        if (time < 0)
            throw new IllegalArgumentException("Illegal execution time.");

        synchronized (queue) {
            if (!thread.newTasksMayBeScheduled)
                throw new IllegalStateException("Timer already cancelled.");

            synchronized (task.lock) {
                if (task.state != TimedTask.VIRGIN)
                    throw new IllegalStateException(
                            "Task already scheduled or cancelled");
                task.nextExecutionTime = time;
                task.period = period;
                task.state = TimedTask.SCHEDULED;
            }

            queue.add(task);
            if (queue.getMin() == task)
                queue.notify();
        }
    }

    /**
     * Terminates this manager, discarding any currently scheduled tasks. Does not
     * interfere with a currently executing task (if it exists). Once a manager
     * has been terminated, its scheduler thread terminates gracefully, and no
     * more tasks may be scheduled on it.
     * <p/>
     * <p/>
     * This method may be called repeatedly; the second and subsequent calls
     * have no effect.
     */
    public void terminate() {
        synchronized (queue) {
            thread.newTasksMayBeScheduled = false;
            queue.clear();
            queue.notify(); // In case queue was already empty.
        }
    }

    /**
     * Removes all cancelled tasks from this timer's task queue. <i>Calling this
     * method has no effect on the behavior of the timer</i>, but eliminates the
     * references to the cancelled tasks from the queue. If there are no
     * external references to these tasks, they become eligible for garbage
     * collection.
     * <p/>
     * <p/>
     * Most programs will have no need to call this method. It is designed for
     * use by the rare application that cancels a large number of tasks. Calling
     * this method trades time for space: the runtime of the method may be
     * proportional to n + c log n, where n is the number of tasks in the queue
     * and c is the number of cancelled tasks.
     * <p/>
     * <p/>
     * Note that it is permissible to call this method from within a a task
     * scheduled on this timer.
     */
    public int purge() {
        int result = 0;

        synchronized (queue) {
            for (int i = queue.size(); i > 0; i--) {
                if (queue.get(i).state == TimedTask.CANCELLED) {
                    queue.quickRemove(i);
                    result++;
                }
            }

            if (result != 0)
                queue.heapify();
        }

        return result;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }


    /**
     * This "helper class" implements the manager's internal task scheduler
     * thread, which waits for tasks on the timer queue, schedules them when
     * they fire, reschedules repeating tasks, and removes cancelled tasks
     * and spent non-repeating tasks from the queue.
     */
    class SchedulerThread extends Thread {

        /**
         * This flag is set to false by the reaper to inform us that there
         * are no more live references to our TimedTaskManager object.
         * Once this flag is true and there are no more tasks in our queue,
         * there is no work left for us to do, so we terminate gracefully.
         * Note that this field is protected by queue's monitor!
         */
        boolean newTasksMayBeScheduled = true;

        /**
         * Our TimedTaskManager's queue.  We store this reference in preference
         * to a reference to the TimedTaskManager so the reference graph remains
         * acyclic. Otherwise, the Timer would never be garbage-collected and this
         * thread would never go away.
         */
        private TimedTaskQueue queue;

        SchedulerThread(TimedTaskQueue queue) {
            this.queue = queue;
        }

        public void run() {
            try {
                mainLoop();
            } finally {
                // Someone killed this Thread, behave as if Timer cancelled
                synchronized (queue) {
                    newTasksMayBeScheduled = false;
                    queue.clear();  // Eliminate obsolete references
                }
            }
        }

        /**
         * The main timer loop.  (See class comment.)
         */
        private void mainLoop() {
            while (true) {
                try {
                    TimedTask task;
                    boolean taskFired;
                    synchronized (queue) {
                        // Wait for queue to become non-empty
                        while (queue.isEmpty() && newTasksMayBeScheduled)
                            queue.wait();
                        if (queue.isEmpty())
                            break; // Queue is empty and will forever remain; die

                        // Queue nonempty; look at first evt and do the right thing
                        long currentTime, executionTime;
                        task = queue.getMin();
                        synchronized (task.lock) {
                            if (task.state == TimedTask.CANCELLED) {
                                queue.removeMin();
                                continue;  // No action required, poll queue again
                            }
                            currentTime = System.currentTimeMillis();
                            executionTime = task.nextExecutionTime;
                            if (taskFired = (executionTime <= currentTime)) {
                                if (task.period == 0) { // Non-repeating, remove
                                    queue.removeMin();
                                    task.state = TimedTask.EXECUTED;
                                } else { // Repeating task, reschedule
                                    queue.rescheduleMin(
                                            task.period < 0 ? (currentTime - task.period)
                                                    : (executionTime + task.period));
                                }
                            }
                        }
                        if (!taskFired) { // Task hasn't yet fired; wait
                            queue.wait(executionTime - currentTime);
                        }
                    }
                    if (taskFired) { // Task fired; run it, holding no locks
                        //task.run();
                        exec.execute(task);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
