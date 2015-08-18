import com.sun.btrace.AnyType;
import com.sun.btrace.annotations.*;
import static com.sun.btrace.BTraceUtils.*;

import java.lang.management.ManagementFactory;
import java.lang.management.GarbageCollectorMXBean;
import java.util.Set;
import java.util.HashSet;
import java.util.List;

/**
 * Common
 * - time(ms),heap(MB),nonheap(MB),total(MB)
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
 * Alarm
 * - Common
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
        if (args[1].toString() ==  "RUNNING")
            println(common() + ",task,start," + args[0] + "," + getGC());
        else
            println(common() + ",task,end," + args[0] + "," + getGC());
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

    /* Shuffle */
    @OnMethod(  // Start
        clazz    = "org.apache.spark.util.collection.ExternalAppendOnlyMap",
        method   = "insertAll",
        location = @Location(value=Kind.ENTRY))
    public static void ExternalAppendOnlyMap_insertAll_entry() {
        println(common() + ",shuffle,map,start");
    }
    @OnMethod(  // Start
        clazz    = "org.apache.spark.util.collection.ExternalSorter",
        method   = "insertAll",
        location = @Location(value=Kind.ENTRY))
    public static void ExternalSorter_insertAll_entry() {
        println(common() + ",shuffle,sorter,start");
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.util.collection.ExternalAppendOnlyMap",
        method   = "insertAll",
        location = @Location(value=Kind.RETURN))
    public static void ExternalAppendOnlyMap_insertAll_return() {
        println(common() + ",shuffle,map,end");
    }
    @OnMethod(  // End
        clazz    = "org.apache.spark.util.collection.ExternalSorter",
        method   = "insertAll",
        location = @Location(value=Kind.RETURN))
    public static void ExternalSorter_insertAll_return() {
        println(common() + ",shuffle,sorter,end");
    }
    @OnMethod(  // Spill
        clazz    = "org.apache.spark.util.collection.ExternalAppendOnlyMap",
        method   = "spill",
        location = @Location(value=Kind.ENTRY))
    public static void ExternalAppendOnlyMap_spill_entry(AnyType[] args) {
        /* args[0] SizeTracker
         */
        org.apache.spark.util.collection.SizeTracker st = (org.apache.spark.util.collection.SizeTracker) args[0];
        println(common() + ",shuffle,map,spill," + st.estimateSize());
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
        println(common() + ",shuffle,sorter,spill," + st.estimateSize());
    }
    @OnMethod(  // Release
        clazz    = "org.apache.spark.shuffle.ShuffleMemoryManager",
        method   = "release",
        location = @Location(value=Kind.ENTRY))
    public static void ShuffleMemoryManager_release_entry(AnyType[] args) {
        /* args[0] Long numBytes
         */
        println(common() + ",shuffle,manager,release," + args[0]);
    }

    /* Alarm for Heap Usage */
    @OnTimer(10)
    public static void alarm() {
        println(common());
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
btracec -cp "$SPARK_HOME/lib/spark-assembly-1.4.0-hadoop2.6.0.jar:$SCALA_HOME/lib/scala-library.jar" ExecutorProbe.java
*/
