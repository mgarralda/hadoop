package eps.mapreduce.job;

    import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * @author Mariano Garralda
 * @author Oscar Ujaque
 */
// Execute JobControl as a Thread
public class JobRunner implements Runnable {
    private JobControl control;

    public JobRunner(JobControl _control) {
        this.control = _control;
    }

    public void run() {
        this.control.run();
    }
}