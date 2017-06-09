package mesosphere.marathon
package metrics

import collection.JavaConversions._

case class ThreadMethodIndex(className: String, methodName: String, fileName: String, lineNumber: Int)

/**
  * A ThreadMethodStats class contains the hit counters for every
  * @param method
  */
class ThreadMethodStats(var method: ThreadMethodIndex = null) {
  private var _methods: Map[ThreadMethodIndex, ThreadMethodStats] = Map[ThreadMethodIndex, ThreadMethodStats]()
  private var _hits: Int = 0

  def next(m: ThreadMethodIndex): ThreadMethodStats = {
    if (!_methods.contains(m)) {
      _methods += (m -> new ThreadMethodStats(m))
    }

    _methods(m)
  }

  def hit(): Unit = {
    _hits += 1
  }

  def methods: Iterable[ThreadMethodStats] = _methods.unzip._2
  def hits: Int = _hits
}

/**
  * A SpinDumpTrace contains the summarised heat map for every thread
  */
class SpinDumpTrace {
  private var _samples: Int = 0;
  private var _threads: Map[String, ThreadMethodStats] = Map[String, ThreadMethodStats]()

  def increment(thread: Thread, stackTrace: Array[StackTraceElement]): Unit = {
    val tname: String = thread.getName
    if (!_threads.contains(tname)) {
      _threads += (tname -> new ThreadMethodStats())
    }

    var curThreadStats: ThreadMethodStats = _threads(tname);
    for (element <- stackTrace) {
      val idx = ThreadMethodIndex(
        className = element.getClassName,
        methodName = element.getMethodName,
        fileName = element.getFileName,
        lineNumber = element.getLineNumber
      )

      // Hit the stack method and continue
      curThreadStats.hit()
      curThreadStats = curThreadStats.next(idx)
    }
  }

  def incSapmles(): Unit = {
    _samples += 1
  }

  def threadStats: Map[String, ThreadMethodStats] = _threads
  def samples: Int = _samples
}

/**
  * Created by icharala on 09/06/17.
  */
object Spindump {

  /**
    * Collects a spin-dump of all threads in marathon
    *
    * This function is blocking the thread for the duration given, sampling the thread stack
    * at the sample rate given and it's composing a spindump structure that can be used to
    * analyze performance errors.
    *
    * @param seconds - For how long to run the spin dump
    * @param samplesPerSecond - How frequently to sample the stack
    * @return The spin-dump report
    */
  def collect(seconds: Int = 1, samplesPerSecond: Int = 500): String = {
    val sampleDelay: Long = 1000 / samplesPerSecond
    val spinDumpTrace: SpinDumpTrace = new SpinDumpTrace
    var t: Thread = null

    for (i <- 0 to seconds * samplesPerSecond) {

      // Sample stack traces from all threads
      spinDumpTrace.incSapmles()
      Thread.getAllStackTraces.toMap.foreach {
        case (thread: Thread, stackTrace: Array[StackTraceElement]) => {
          if (t == null) {
            t = thread
          }
          spinDumpTrace.increment(thread, stackTrace.reverse)
        }
      }

      // Throttle sample rate
      Thread.sleep(sampleDelay)
    }

    stringifyDump(spinDumpTrace)
  }

  def stringifyThreadStats(stats: ThreadMethodStats, indent: Int = 1): String = {
    var dump = s"${Seq.fill(indent)(" ").mkString} ${stats.hits} ${stats.method.className}.${stats.method.methodName}\n"
    for (method <- stats.methods) {
      dump += stringifyThreadStats(method, indent + 1)
    }

    dump
  }

  def stringifyDump(sd: SpinDumpTrace): String = {
    var dump: String = s"Collected ${sd.samples} samples in total\n\n"

    for ((threadName, threadStats) <- sd.threadStats) {
      dump += s"Thread $threadName\n"
      for (method <- threadStats.methods) {
        dump += stringifyThreadStats(method)
      }
      dump += "\n"
    }

    dump
  }

}
