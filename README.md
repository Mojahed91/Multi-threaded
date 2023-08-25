
# IMPLEMENTATION

Our framework runs in the RunMapReduceFramework function in three stages:
The first stage is creating the seven threads that handle the mapping, which each one of which handles a chunk of given data at the same time, by mutexes we make sure that the share data are available by one of the threads.
In the second stage after the creation of the map threads we create the shuffle thread that works in parallel, in the end of this stage we join each map and shuffle threads.
The last stage is a reduction which is similar to the mapping stage.

So that each one of the threads takes a chunk of share data we defined a variable with name position and by the mutex, we blocked this variable to be updated.

All the data that we got from the user map function which consists of pairs of k1,v1, we decide to put in the map data structure container the key of the map container is the index of the thread and the value is the container that this thread adding to it the pairs (k1,v1).  we decided to create a vector in it we inserted mutexes the same number as the number of the threads, and each mutex locks the vector related to a specific thread in the map container.

<img width="927" alt="Screen Shot 2023-08-25 at 15 29 48" src="https://github.com/Mojahed91/Multi-threaded/assets/129369338/2b6d375f-799a-47bc-b599-1ad0e5657f1e">

and we created a shuffleMap, the key for the shuffleMap is the (K2) and the value is a vector to all the values that are a couple with (K2). we created a structure to reduce similar to the structure of the map. In the end we collected all the couples (k3,v3) into one vector and we return it by the function:
 "RunMapReduceFramework". Inside the file "search.cpp" we created 6 classes that are completion to 6 classes that we were given inside the file "mapreduceclient.h" ; inside the 6 classes, we added attribute to the name of the directory
 and the name of the file and the value of the directory and the value of the file.
 and we implemented the class ("mapreducebase"), in the first function (map) we iterated over all the files inside the directory and we called for each file a function "emit2".
 in the second function (reduce) we iterated over all the values and we sum them inside the vector that we get and we called for each file a function "emit3".
