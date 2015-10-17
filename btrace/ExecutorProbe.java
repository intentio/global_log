import com.sun.btrace.AnyType;
import com.sun.btrace.annotations.*;
import static com.sun.btrace.BTraceUtils.*;

import java.lang.management.ManagementFactory;
import java.lang.management.GarbageCollectorMXBean;
import com.sun.management.OperatingSystemMXBean;

import java.util.Set;
import java.util.HashSet;
import java.util.List;

/**
 * Alarm
 * - Common
 *
 * Common
 * - time(ms),Memory,CPU
 *
 * Memory
 * - heap(MB),nonheap(MB),total(MB)
 *
 * CPU
 * - systemCpuLoad,processCpuLoad
 *
 * Task start/end
 * - Common,task,start,taskId,GC
 * - Common,task,end,taskId,GC
 *
 * Persist memory/disk/offheap
 * - Common,persist,memory,size(B)
 *
 * Shuffle start/end/spill
 * - Common,shuffle,map,start
 * - Common,shuffle,sorter,start
 * - Common,shuffle,map,end
 * - Common,shuffle,sorter,end
 * - Common,shuffle,map,spill,size(B)
 * - Common,shuffle,sorter,spill,size(B)
 * - Common,shuffle,manager,release,size(B)
 *
 * GC
 * - count,time(ms)
 */

@BTrace(unsafe = true)
public class ExecutorProbe {
    /* Task */
    @OnMethod(  // Start/End
        clazz    = "org.apache.spark.executor.CoarseGrainedExecutorBackend",
        method   = "statusUpdate",
        location = @Location(value=Kind.ENTRY))
    public static void CoarseGrainedExecutorBackend_statusUpdate(AnyType[] args) {
        /* args[0] Int taskId
         * args[1] TaskState (RUNNING, FINISHED, FAILED, KILLED)
         */
        if (args[1].toString() ==  "RUNNING")
            println(getCommon() + ",task,start," + args[0] + "," + getGC());
        else
            println(getCommon() + ",task,end," + args[0] + "," + getGC());
    }

    /* Persist */
    @OnMethod(  // Memory
        clazz    = "org.apache.spark.storage.MemoryEntry",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void MemoryEntry_init_entry(@Self AnyType self, AnyType[] args) {
        /* args[1] Long size
         */
        println(getCommon() + ",persist,memory," + args[1]);
    }

    /* Shuffle */
    @OnMethod(  // Start
        clazz    = "org.apache.spark.util.collection.ExternalAppendOnlyMap",
        method   = "insertAll",
        location = @Location(value=Kind.ENTRY))
    public static void ExternalAppendOnlyMap_insertAll_entry() {
        println(getCommon() + ",shuffle,map,start");
    }
    @OnMethod(  // Start
        clazz    = "org.apache.spark.util.collection.ExternalSorter",
        method   = "insertAll",
        location = @Location(value=Kind.ENTRY))
    public static void ExternalSorter_insertAll_entry() {
        println(getCommon() + ",shuffle,sorter,start");
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.util.collection.ExternalAppendOnlyMap",
        method   = "insertAll",
        location = @Location(value=Kind.RETURN))
    public static void ExternalAppendOnlyMap_insertAll_return() {
        println(getCommon() + ",shuffle,map,end");
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.util.collection.ExternalSorter",
        method   = "insertAll",
        location = @Location(value=Kind.RETURN))
    public static void ExternalSorter_insertAll_return() {
        println(getCommon() + ",shuffle,sorter,end");
    }
    @OnMethod(  // Spill
        clazz    = "org.apache.spark.util.collection.ExternalAppendOnlyMap",
        method   = "spill",
        location = @Location(value=Kind.ENTRY))
    public static void ExternalAppendOnlyMap_spill_entry(AnyType[] args) {
        /* args[0] SizeTracker
         */
        org.apache.spark.util.collection.SizeTracker st = (org.apache.spark.util.collection.SizeTracker) args[0];
        println(getCommon() + ",shuffle,map,spill," + st.estimateSize());
    }
    @OnMethod(  // Spill
        clazz    = "org.apache.spark.util.collection.ExternalSorter",
        method   = "spill",
        location = @Location(value=Kind.ENTRY))
    public static void ExternalSorter_spill_entry(AnyType[] args) {
        /* args[0]
         * PartitionedPairBuffer extends WritablePartitionedPairCollection with SizeTracker
         * PartitionedSerializedPairBuffer extends WritablePartitionedPairCollection with SizeTracker
         */
        org.apache.spark.util.collection.SizeTracker st = (org.apache.spark.util.collection.SizeTracker) args[0];
        println(getCommon() + ",shuffle,sorter,spill," + st.estimateSize());
    }
    @OnMethod(  // Release
        clazz    = "org.apache.spark.shuffle.ShuffleMemoryManager",
        method   = "release",
        location = @Location(value=Kind.ENTRY))
    public static void ShuffleMemoryManager_release_entry(AnyType[] args) {
        /* args[0] Long numBytes
         */
        println(getCommon() + ",shuffle,manager,release," + args[0]);
    }

    /* Alarm */
    @OnTimer(10)
    public static void alarm() {
        println(getCommon());
    }

    /* GC */
    private static StringBuilder getGC() {
        // count,time
        long totalCount = 0;
        long totalTime = 0;

        List<GarbageCollectorMXBean> mxBeans = ManagementFactory.getGarbageCollectorMXBeans();

        for (GarbageCollectorMXBean gc : mxBeans) {
            long count = gc.getCollectionCount();
            if (count >= 0) {
                totalCount += count;
                totalTime  += gc.getCollectionTime();
                //println("# GC Name = " + gc.getName());
                //println("# totalCount = " + totalCount);
                //println("# totalTime = " + totalTime);
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append(totalCount).append(",").append(totalTime);
        return sb;
    }

    /* Every event needs to print out this string. */
    private static String getCommon() {
        return String.valueOf(Sys.VM.vmUptime()) + "," + getMemory() + "," + getCpu();
    }

    /* Memory */
    private static String getMemory() {
        long heap = heapUsage().getUsed();
        long nonheap = nonHeapUsage().getUsed();
        return String.valueOf(convert(heap)) + "," 
            + String.valueOf(convert(nonheap)) + "," 
            + String.valueOf(convert(heap + nonheap));
    }

    private static double convert(long m) {
        return (double)(Math.round( (m / 1024.0 / 1024.0) * 100.0 )) / 100.0;  // B -> MB with double precision
    }

    /* CPU
     * getProcessCpuLoad() - returns the "recent cpu usage" for the Java Virtual Machine process. [0,1]
     * getSystemCpuLoad()  - returns the "recent cpu usage" for the whole system. [0,1] */
    private static String getCpu() {
        OperatingSystemMXBean osMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        double p = osMXBean.getProcessCpuLoad();
        double s = osMXBean.getSystemCpuLoad();
        return String.format("%.2f", p) + "," + String.format("%.2f", s);
    }
}

/*
btracec -cp "$SPARK_HOME/lib/spark-assembly-1.5.1-hadoop2.6.0.jar:$SCALA_HOME/lib/scala-library.jar" ExecutorProbe.java
*/
