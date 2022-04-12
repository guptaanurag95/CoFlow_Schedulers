# Inter-Coflow Scheduling to Minimize CCT

I have inplemented following three inter-coflow scheduling algorithms described in the paper "Efficient Coflow Scheduling with Varys".

1. Shortest-Coflow First (SCF) - Schedules the Co-Flow having the shortest flow first.
2. Narrowest-Coflow-First (NCF) - Schedules the Co-Flow having the least number of parallel flows.
3. Smallest-Effective-Bottleneck-First (SEBF) - Considers a coflow’s length, width, size, and skew to schedule the Co-Flow.

## Instructions to Run the project

To run the program, follow the commands given below.
```
python3 CoFlowSim.py <inputFile> <Algo>
```

`<inputFile>` is a file containing the traces for Jobs. 2 files are present in the repo - sampleTrace1.txt and sampleTrace2.txt.

`<Algo>` can be SCF, NCF or SEBF for the given 3 scheduling algorithms.

The program will output the average CCT.

## Trace File

Trace file is taken from the Coflow-Benchmark, which is also used in the paper. Arrival times of the jobs are modified to reduce simulation time. The format of the trace file is as bellow.

```
Line 1: <Number of ports in the fabric> <Number of coflows below (one per line)>
Line i: <Coflow ID> <Arrival time (ms)> <Number of mappers> <Location of map-m> <Number of reducers> <Location of reduce-r:Shuffle megabytes of reduce-r>
```

## Reference

1. https://www.mosharaf.com/wp-content/uploads/varys-sigcomm14.pdf
2. https://github.com/coflow/coflow-benchmark
3. https://github.com/coflow/coflowsim