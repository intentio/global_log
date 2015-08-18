import com.sun.btrace.AnyType;
import com.sun.btrace.annotations.*;
import static com.sun.btrace.BTraceUtils.*;

import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskInfo;

/**
 * Common
 * - time(ms),heap(MB),nonheap(MB),total(MB)
 *
 * Job start/end
 * - Common,job,start,jobId
 * - Common,job,end,jobId
 *
 * Stage start/end
 * - Common,stage,start
 * - Common,stage,end
 *
 * Task start/end
 * - Common,task,start,stageId,stageAttemptId,partitionIndex.taskAttemptId
 * - Common,task,end,stageId,stageAttemptId,partitionIndex.taskAttempt
 *
 * Persist memory/disk/offheap
 * - Common,persist,memory,size(B)
 *
 * Alarm
 * - Common
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
        println(common() + ",job,start," + args[0]);
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.scheduler.SparkListenerJobEnd",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void job_end(AnyType[] args) {
        /* args[0] Int jobId
         */
        println(common() + ",job,end," + args[0]);
    }

    /* Stage */
    @OnMethod(  // Start
        clazz    = "org.apache.spark.scheduler.SparkListenerStageSubmitted",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void stage_submitted(AnyType[] args) {
        println(common() + ",stage,start");
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.scheduler.SparkListenerStageCompleted",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void stage_completed(AnyType[] args) {
        println(common() + ",stage,end");
    }

    /* Task */
    @OnMethod(  // Start
        clazz    = "org.apache.spark.scheduler.SparkListenerTaskStart",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void task_start(AnyType[] args) {
        Object stageId = args[0];
        Object stageAttemptId = args[1];
        TaskInfo taskInfo = (TaskInfo) args[2];

        println(common() + ",task,start," + stageId + "," + stageAttemptId + "," + taskInfo.id());
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.scheduler.SparkListenerTaskEnd",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void task_end(@Self AnyType self, AnyType[] args) {
        Object stageId = args[0];
        Object stageAttemptId = args[1];
        TaskInfo taskInfo = (TaskInfo) args[4];

        println(common() + ",task,end," + stageId + "," + stageAttemptId + "," + taskInfo.id());
    }

    /* Persist */
    @OnMethod(  // Memory
        clazz    = "org.apache.spark.storage.MemoryEntry",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void MemoryEntry_init_entry(@Self AnyType self, AnyType[] args) {
        /* args[1] Long size
         */
        println(common() + ",persist,memory," + args[1]);
    }

    /* Alarm for Heap Usage */
    @OnTimer(10)
    public static void alarm() {
        println(common());
    }

    private static double convert(long m) {
        return (double)(Math.round( (m / 1024.0 / 1024.0) * 100.0)) / 100.0;  // B -> MB with double precision
    }

    /* Every event needs to print out this string.
     * time,heap(MB),nonheap(MB),total(MB) */
    private static String common() {
        long heap = heapUsage().getUsed();
        long nonheap = nonHeapUsage().getUsed();
        return String.valueOf(Sys.VM.vmUptime()) + "," + String.valueOf(convert(heap)) + "," + String.valueOf(convert(nonheap)) + "," + String.valueOf(convert(heap + nonheap));
    }
}

/*
btracec -cp "$SPARK_HOME/lib/spark-assembly-1.4.0-hadoop2.6.0.jar:$SCALA_HOME/lib/scala-library.jar" DriverProbe.java

spark_submit \
--class MemoryOnly \
--driver-java-options "-javaagent:$BTRACE_HOME/build/btrace-agent.jar=unsafe=true,scriptOutputFile=/home/ec2-user/btracelog,script=/home/ec2-user/DriverProbe.class" \
sparkapp_2.11-0.1.jar
*/
