Benchmarks here are generally in src/main and can be run with `benchmark/jmh:run`
within SBT.

See here for more details:
[SBT-JMH](https://github.com/ktoso/sbt-jmh)
[JMS-Samples](http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/)

## Benchmarks

### Health Check Actor

This benchmark requires a running HTTP server on `127.0.0.1:8080`. One can start it with `./scripts/minimal_ping_server.sh`.

The benchmark itself can then be started with `sbt "benchmark/jmh:run -i 1 -wi 1 HealthCheckActorBenchmark.*"`.
