import com.sun.btrace.AnyType;
import com.sun.btrace.annotations.*;
import static com.sun.btrace.BTraceUtils.*;

import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskInfo;

/**
 * Job start/end
 * - time(ms),heap(MB),job,start,jobId
 * - time(ms),heap(MB),job,end,jobId
 *
 * Stage start/end
 * - time(ms),heap(MB),stage,start
 * - time(ms),heap(MB),stage,end
 *
 * Task start/end
 * - time(ms),heap(MB),task,start,stageId,stageAttemptId,partitionIndex.taskAttemptId
 * - time(ms),heap(MB),task,end,stageId,stageAttemptId,partitionIndex.taskAttempt
 *
 * Persist memory/disk/offheap
 * - time(ms),heap(MB),persist,memory,size(B)
 *
 * Alarm
 * - time(ms),heap(MB)
 */

@BTrace(unsafe = true)
public class DriverProbe {
    /* Job */
    @OnMethod(  // Start
        clazz    = "org.apache.spark.scheduler.SparkListenerJobStart",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void job_start(AnyType[] args) {
        /* args[0] Int jobId
         */
        // time,heap(MB),job,start,jobId
        println(Sys.VM.vmUptime() + "," + heapUsed() + ",job,start," + args[0]);
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.scheduler.SparkListenerJobEnd",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void job_end(AnyType[] args) {
        /* args[0] Int jobId
         */
        // time,heap(MB),job,end,jobId
        println(Sys.VM.vmUptime() + "," + heapUsed() + ",job,end," + args[0]);
    }

    /* Stage */
    @OnMethod(  // Start
        clazz    = "org.apache.spark.scheduler.SparkListenerStageSubmitted",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void stage_submitted(AnyType[] args) {
        // time,heap(MB),stage,start
        println(Sys.VM.vmUptime() + "," + heapUsed() + ",stage,start");
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.scheduler.SparkListenerStageCompleted",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void stage_completed(AnyType[] args) {
        // time,heap(MB),stage,end
        println(Sys.VM.vmUptime() + "," + heapUsed() + ",stage,end");
    }

    /* Task */
    @OnMethod(  // Start
        clazz    = "org.apache.spark.scheduler.SparkListenerTaskStart",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void task_start(AnyType[] args) {
        // time,heap(MB),task,start,stageId,stageAttemptId,partitionIndex.taskAttemptId
        Object stageId = args[0];
        Object stageAttemptId = args[1];
        TaskInfo taskInfo = (TaskInfo) args[2];

        println(Sys.VM.vmUptime() + "," + heapUsed() + ",task,start," + stageId + "," + stageAttemptId + "," + taskInfo.id());
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.scheduler.SparkListenerTaskEnd",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void task_end(@Self AnyType self, AnyType[] args) {
        // time,heap(MB),task,end,stageId,stageAttemptId,partitionIndex.taskAttemptId
        Object stageId = args[0];
        Object stageAttemptId = args[1];
        TaskInfo taskInfo = (TaskInfo) args[4];

        println(Sys.VM.vmUptime() + "," + heapUsed() + ",task,end," + stageId + "," + stageAttemptId + "," + taskInfo.id());
    }

    /* Persist */
    @OnMethod(  // Memory
        clazz    = "org.apache.spark.storage.MemoryEntry",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void MemoryEntry_init_entry(@Self AnyType self, AnyType[] args) {
        /* args[1] Long size
         */
        // time,heap(MB),persist,memory,size
        println(Sys.VM.vmUptime() + "," + heapUsed() + ",persist,memory," + args[1]);
    }

    /* Alarm for Heap Usage */
    @OnTimer(10)
    public static void alarm() {
        println(Sys.VM.vmUptime() + "," +  heapUsed());
    }

    private static double heapUsed() {
        double used = heapUsage().getUsed() / 1024.0 / 1024.0;  // MB
        used = (double)(Math.round(used * 100.0)) / 100.0;
        return used;
    }
}

/*
btracec -cp "$SPARK_HOME/lib/spark-assembly-1.4.0-hadoop2.6.0.jar:$SCALA_HOME/lib/scala-library.jar" DriverProbe.java

spark_submit \
--class MemoryOnly \
--driver-java-options "-javaagent:$BTRACE_HOME/build/btrace-agent.jar=unsafe=true,scriptOutputFile=/home/ec2-user/out.trace,script=/home/ec2-user/DriverProbe.class" \
sparkapp_2.11-0.1.jar
*/
