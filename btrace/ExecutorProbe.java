import com.sun.btrace.AnyType;
import com.sun.btrace.annotations.*;
import static com.sun.btrace.BTraceUtils.*;

import java.lang.management.ManagementFactory;
import java.lang.management.GarbageCollectorMXBean;
import java.util.Set;
import java.util.HashSet;
import java.util.List;

/**
 * Task start/end
 * - time(ms),heap(MB),task,start,taskId,GC
 * - time(ms),heap(MB),task,end,taskId,GC
 *
 * Persist memory/disk/offheap
 * - time(ms),heap(MB),persist,memory,size(B)
 *
 * Shuffle start/end/spill
 * - time(ms),heap(MB),shuffle,start
 * - time(ms),heap(MB),shuffle,end
 * - time(ms),heap(MB),shuffle,spill,size(B)
 *
 * Alarm
 * - time(ms),heap(MB)
 *
 * GC
 * - minorcount,minortime(ms),majorcount,majortime(ms)
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
        // time,heap(MB),task,start,taskId,GC
        // time,heap(MB),task,end,taskId,GC
        if (args[1].toString() ==  "RUNNING")
            println(Sys.VM.vmUptime() + "," + heapUsed() + ",task,start," + args[0] + "," + getGC());
        else
            println(Sys.VM.vmUptime() + "," + heapUsed() + ",task,end," + args[0] + "," + getGC());
    }

    /* Persist */
    @OnMethod(  // Memory
        clazz    = "org.apache.spark.storage.MemoryEntry",
        method   = "<init>",
        location = @Location(value=Kind.ENTRY))
    public static void MemoryEntry_init_entry(@Self AnyType self, AnyType[] args) {
        /* args[1] Long size
         */
        // time,heap(MB),persist,memory,size(B)
        println(Sys.VM.vmUptime() + "," + heapUsed() + ",persist,memory," + args[1]);
    }

    /* Shuffle */
    @OnMethod(  // Start
        clazz    = "org.apache.spark.util.collection.ExternalAppendOnlyMap",
        method   = "insertAll",
        location = @Location(value=Kind.ENTRY))
    public static void ExternalAppendOnlyMap_insertAll_entry() {
        // time,heap(MB),shuffle,start
        println(Sys.VM.vmUptime() + "," + heapUsed() + ",shuffle,start,map");
    }
    @OnMethod(  // Start
        clazz    = "org.apache.spark.util.collection.ExternalSorter",
        method   = "insertAll",
        location = @Location(value=Kind.ENTRY))
    public static void ExternalSorter_insertAll_entry() {
        // time,heap(MB),shuffle,start
        println(Sys.VM.vmUptime() + "," + heapUsed() + ",shuffle,start,sorter");
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.util.collection.ExternalAppendOnlyMap",
        method   = "insertAll",
        location = @Location(value=Kind.RETURN))
    public static void ExternalAppendOnlyMap_insertAll_return() {
        // time,heap(MB),shuffle,end
        println(Sys.VM.vmUptime() + "," + heapUsed() + ",shuffle,end,map");
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.util.collection.ExternalSorter",
        method   = "insertAll",
        location = @Location(value=Kind.RETURN))
    public static void ExternalSorter_insertAll_return() {
        // time,heap(MB),shuffle,end
        println(Sys.VM.vmUptime() + "," + heapUsed() + ",shuffle,end,sorter");
    }
    @OnMethod(  // Spill
        clazz    = "org.apache.spark.shuffle.ShuffleMemoryManager",
        method   = "release",
        location = @Location(value=Kind.ENTRY))
    public static void ShuffleMemoryManager_release_entry(@Self AnyType self, AnyType[] args) {
        /* args[0] Long numBytes
         */
        // time,heap(MB),shuffle,spill,size
        println(Sys.VM.vmUptime() + "," + heapUsed() + ",shuffle,spill," + args[0]);
    }

    /* Alarm for Heap Usage */
    @OnTimer(10)
    public static void alarm() {
        // time,heap(MB)
        println(Sys.VM.vmUptime() + "," +  heapUsed());
    }

    /* GC */
    private static StringBuilder getGC() {
        // minorcount,minortime,majorcount,majortime
        Set<String> YOUNG_GC = new HashSet<String>(3);
        Set<String> OLD_GC = new HashSet<String>(3);

        YOUNG_GC.add("PS Scavenge");
        YOUNG_GC.add("ParNew");
        YOUNG_GC.add("G1 Young Generation");

        OLD_GC.add("PS MarkSweep");
        OLD_GC.add("ConcurrentMarkSweep");
        OLD_GC.add("G1 Old Generation");

        long minorCount = 0;
        long minorTime = 0;
        long majorCount = 0;
        long majorTime = 0;
        long unknownCount = 0;
        long unknownTime = 0;

        List<GarbageCollectorMXBean> mxBeans = ManagementFactory.getGarbageCollectorMXBeans();

        for (GarbageCollectorMXBean gc : mxBeans) {
            long count = gc.getCollectionCount();
            if (count >= 0) {
                if (YOUNG_GC.contains(gc.getName())) {
                    minorCount += count;
                    minorTime += gc.getCollectionTime();
                } else if (OLD_GC.contains(gc.getName())) {
                    majorCount += count;
                    majorTime += gc.getCollectionTime();
                } else {
                    unknownCount += count;
                    unknownTime += gc.getCollectionTime();
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append(minorCount).append(",").append(minorTime).append(",")
            .append(majorCount).append(",").append(majorTime);
        return sb;
    }

    /* The size of used heap of JVM in MB */
    private static double heapUsed() {
        double used = heapUsage().getUsed() / 1024.0 / 1024.0;  // MB
        used = (double)(Math.round(used * 100.0)) / 100.0;
        return used;
    }

}

/*
btracec -cp "$SPARK_HOME/lib/spark-assembly-1.4.0-hadoop2.6.0.jar:$SCALA_HOME/lib/scala-library.jar" ExecutorProbe.java
*/
