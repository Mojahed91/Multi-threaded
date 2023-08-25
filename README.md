# MapReduce Multi-threaded Framework (MRMTF)

 This README provides an overview of our multi-threaded MapReduce framework implementation (MRMTF). This framework is designed to simplify the processing and generation of large datasets using the MapReduce paradigm while taking full advantage of multi-threading capabilities. The MRMTF is a tool for developers to process vast amounts of data using the power of multi-threading. It brings the simplicity of the MapReduce programming model and combines it with the speedup achieved through parallel execution on multi-core systems.


## Implementation Overview


Our framework operates within the RunMapReduceFramework function and executes in three main stages:

1- Mapping Stage:

  * Seven threads are created to handle the mapping. Each thread processes a distinct chunk of data concurrently.
  * Mutexes ensure that shared data is accessed by only one thread at a time.
    
2- Shuffling Stage:

  * Upon the completion of the mapping stage, a shuffle thread is created and executed concurrently.
  * At the end of this stage, all map and shuffle threads are joined.
    
3- Reduction Stage:

  * Operates similarly to the mapping stage, with each thread processing a chunk of shared data.

We use a position variable to ensure each thread processes a unique chunk of data. This variable is protected by a mutex to prevent simultaneous updates.


##  Data Structures

The data from the user's map function comprises pairs of (k1, v1). These are stored in a map data structure. The key for this structure is the thread index, and the value is a container that the thread populates with (k1, v1) pairs.

<img width="927" alt="Screen Shot 2023-08-25 at 15 29 48" src="https://github.com/Mojahed91/Multi-threaded/assets/129369338/2b6d375f-799a-47bc-b599-1ad0e5657f1e">

Caption: Data flow and structure in the MapReduce framework.


## Additional Implementation Details
In search.cpp, we've created six supplementary classes to the ones provided in mapreduceclient.h. These classes have attributes for directory and file names, and directory and file values.

The mapreducebase class is implemented as follows:

* In the map function, we iterate over all files in a directory and call the emit2 function for each file.
* In the reduce function, we iterate over all values, sum them, and call the emit3 function for each aggregated value.

