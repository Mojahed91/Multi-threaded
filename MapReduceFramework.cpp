#include "MapReduceFramework.h"
#include <sys/time.h>
#include <iostream>
#include <vector>
#include <map>
#include <deque>
#include <fstream>
#include <semaphore.h>
#include <algorithm>
#include <unistd.h>

#define CHUNK 10
#define SEC_TO_MICRO 1000000
#define MICRO_TO_NANO 1000
#define GET_ELAPSED_TIME (((logEnd.tv_sec - logBegin.tv_sec) * SEC_TO_MICRO) + (logEnd.tv_usec - logBegin.tv_usec)) * MICRO_TO_NANO

bool CREATE = true;
bool TERMINATE = false;
bool reduceThreads = false;

# define GET_ELAPSED_TIME ( ((logEnd.tv_sec - logBegin.tv_sec) * SEC_TO_MICRO) \
+(logEnd.tv_usec - logBegin.tv_usec) ) * MICRO_TO_NANO;

typedef std::pair<k2Base*, v2Base*> MID_ITEM;

sem_t semaphore;
unsigned int position;
bool mapFinished;
bool deleteV2K2;

unsigned int sizeInItemVector;
unsigned int sizeThreads;
std::ofstream ofs;

pthread_mutex_t logMutex;
pthread_mutex_t mapMutex;
pthread_mutex_t shuffleMutex;
pthread_mutex_t reduceMutex;



struct shuffleComp{
  bool operator()(const k2Base* first, const k2Base* second)
  {
    return (*first)<(*second);
  }
};


struct reduceComp{
  bool operator()(const int first, const int second)
  {
    return first != second;
  }
};



// ---------------------------- Data Structures -------------------------------
MapReduceBase* objMapReduce;

std::vector<IN_ITEM> inItemVector;

std::map<int, std::vector<MID_ITEM>> mapContainerMap;
std::vector<pthread_mutex_t> mapContainerMutex;

std::map<k2Base*, std::vector<v2Base*>, shuffleComp> shuffleMap;

std::vector<k2Base*> prepareToReduce;

std::map<int, std::vector<OUT_ITEM>> reduceContainerMap;
std::vector<OUT_ITEM> mergeReduceContainer;
std::vector<pthread_t> threads;
// -----------------------------------------------------------------------------



/**
* returns curr time string for log
*/
const std::string getCurrentTime()
{
  time_t currentTime = time(0);
  struct tm timeStruct = *localtime(&currentTime);
  char buffer[80];
  
  strftime(buffer, sizeof(buffer), "%d.%m.%Y %X", &timeStruct);
  return std::string(buffer);
}

/**
* print thread for log
*/
void logCreateThread(std::string ThreadName)
{
  ofs << "Thread " << ThreadName << " created [" << getCurrentTime() << "]\n";
}

/**
* print time of thread for log
*/
void logTerminateThread(std::string ThreadName)
{
  ofs << "Thread " << ThreadName << " terminated [" << getCurrentTime() << "]\n";
}


void checkSysCall(int res, std::string funcName)
{
  if (res < 0)
  {
    std::cout << "MapReduceFramework Failure: " << funcName
    << " failed." << std::endl;
    exit(1);
  }
}

/**
* check if return value of system call is invalid, print error to stderr
*/
void checkLockMutex(int res, std::string funcName)
{
  if (res != 0)
  {
    std::cout << "MapReduceFramework Failure: " << funcName
    << " failed." << std::endl;
    exit(1);
  }
}


void lockThread(pthread_mutex_t thread)
{
  int lockRes = pthread_mutex_lock(&thread);
  checkLockMutex(lockRes, "pthread_mutex_lock");
}
  
void unlockThread(pthread_mutex_t thread)
{
  int lockRes = pthread_mutex_lock(&thread);
  checkLockMutex(lockRes, "pthread_mutex_lock");
}


void printLog(std::string state, bool alive)
{
  //print create to log
  lockThread(logMutex);
  if(alive == CREATE)
    logCreateThread(state);
  else
    logTerminateThread(state);
    unlockThread(logMutex);
}



int indexOfThread(pthread_t thread)
{

  for (unsigned int i = 0; i < sizeThreads; ++i)
  {
    if (threads[i] == thread)
    {
      return i;
    }
  }
  return -1;
}

void Emit2(k2Base* key, v2Base* val)
{
  int pos = indexOfThread(pthread_self());
  int res = pthread_mutex_lock(&mapContainerMutex[pos]);
  checkLockMutex(res, "pthread_mutex_lock");
  mapContainerMap[pos].push_back(std::make_pair(key, val));
  res = pthread_mutex_unlock(&mapContainerMutex[pos]);
  checkLockMutex(res, "pthread_mutex_lock");
}

/**
* add pair of k3Base and v3Base to matcing container of execReduce thread
*/
void Emit3 (k3Base* key, v3Base* val)
{
  
  int pos = indexOfThread(pthread_self());
  reduceContainerMap[pos].push_back(std::make_pair(key, val));
}

bool k2comp(const k2Base* first, const k2Base* second){
  
  return (*first)<(*second);
}


int counter = 0;


void mergeContainerMap(){
  
  for (unsigned int i = 0; i < sizeThreads; ++i)
  {
    int res = pthread_mutex_lock(&mapContainerMutex[i]);
    checkLockMutex(res, "pthread_mutex_lock");
    std::vector<MID_ITEM> threadPairsVector;
    MID_ITEM frontPair;
    threadPairsVector = mapContainerMap[i];
    while(!threadPairsVector.empty())
    {
      frontPair = threadPairsVector.back();
      threadPairsVector.pop_back();
      shuffleMap[frontPair.first].push_back(frontPair.second);
      }
      mapContainerMap[i].clear();
      res = pthread_mutex_unlock(&mapContainerMutex[i]);
      checkLockMutex(res, "pthread_mutex_lock");
    }

}

void fillkey2vector(){
  
  for(auto const element : shuffleMap) 
  {
    prepareToReduce.push_back(element.first);
  }
}


void *shuffle(void*)
{
  printLog("Shuffle", CREATE);
  
  while(mapFinished)
  {
    sem_wait(&semaphore);
    mergeContainerMap();
  }

  printLog("Shuffle", TERMINATE);
  pthread_exit(NULL);
}
  
void updatePosition(){
  for (unsigned int i = 0; i < CHUNK && position < sizeInItemVector ; ++i)
  {
    position++;
  }
}


void* execMap(void*)
{
  printLog("ExecMap", CREATE);
  
  int lowerPosition, upperPosition;
  while (position < sizeInItemVector)
  {
    int res = pthread_mutex_lock(&mapMutex);
    checkLockMutex(res, "pthread_mutex_lock");
  
    
    
    lowerPosition = position;
    updatePosition();
    upperPosition = position;
    for (; lowerPosition < upperPosition; ++lowerPosition)
    {  
      k1Base * k1 = inItemVector[lowerPosition].first;
      v1Base * v1 = inItemVector[lowerPosition].second;
      objMapReduce->Map(k1 ,v1);
    }
    sem_post(&semaphore);
    res = pthread_mutex_unlock(&mapMutex);
    checkLockMutex(res, "pthread_mutex_lock");
    
  }
  
  printLog("ExecMap", TERMINATE);
  pthread_exit(NULL);
}



void* execReduce(void*)
{
  printLog("ExecReduce", CREATE);
  
  int lowerPosition, upperPosition;
  while (position < prepareToReduce.size())
  {

    int res = pthread_mutex_lock(&reduceMutex);
    checkLockMutex(res, "pthread_mutex_lock");
    lowerPosition = position;
    updatePosition();
    upperPosition = position;
    res = pthread_mutex_unlock(&reduceMutex);
    checkLockMutex(res, "pthread_mutex_lock");

    while( lowerPosition < upperPosition)
    {
      k2Base* k2 = prepareToReduce[lowerPosition];
      objMapReduce->Reduce( k2, shuffleMap[k2]);
      lowerPosition++;
    }
  }

  printLog("ExecReduce", TERMINATE);
  pthread_exit(NULL);
}


bool pairCompare(const OUT_ITEM& left, const OUT_ITEM& right)
{
  return (*left.first) < (*right.first);
}


void destroyMutexes()
{
  int res = pthread_mutex_destroy(&logMutex);
  checkSysCall(res, "pthread_mutex_destroy");
  res = pthread_mutex_destroy(&mapMutex);
  
  
  
  checkSysCall(res, "pthread_mutex_destroy");
  res = pthread_mutex_destroy(&shuffleMutex);
  checkSysCall(res, "pthread_mutex_destroy");
  res = pthread_mutex_destroy(&reduceMutex);
}

void mergeReduce(){

  for (unsigned int i = 0; i < sizeThreads; ++i)
  {
    for (unsigned int j = 0; j < reduceContainerMap[i].size(); ++j)
    {
    mergeReduceContainer.push_back(reduceContainerMap[i][j]);
    }
  }
}


void destroyAll()
{
  destroyMutexes();
  if(deleteV2K2)
  {
    for (unsigned int i = 0; i < mapContainerMap.size(); ++i)
    {
      mapContainerMap[i].clear();
      int res = pthread_mutex_destroy(&mapContainerMutex[i]);
      checkSysCall(res, "pthread_mutex_destroy");
    }
    mapContainerMap.clear();
    
    for (unsigned int j = 0; j < reduceContainerMap.size(); ++j)
    {
      reduceContainerMap[j].clear();
    }
    reduceContainerMap.clear();
    
    std::map<k2Base *, std::vector<v2Base *>, shuffleComp>::iterator it = shuffleMap.begin();
    for ( ; it != shuffleMap.end() ; ++it)
    {
      it->second.clear();
    }
    shuffleMap.clear();
  }
}


/**
* init log
*/
void initializeLog(timeval& begin)
{
  std::string currentWorkDir(__FILE__);
  std::string logPath =
  currentWorkDir.substr(0, currentWorkDir.find_last_of("\\/") + 1) + "MapReduceFramework.log";
  ofs.open(logPath, std::ofstream::app);
  ofs << "runMapReduceFramework started with " << sizeThreads << " threads\n";
  
  gettimeofday(&begin, NULL);
}



OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec, int multiThreadLevel, bool autoDeleteV2K2)
{

  objMapReduce = &mapReduce;
  inItemVector = itemsVec;
  sizeInItemVector = itemsVec.size();
  sizeThreads = multiThreadLevel;
  position = 0;
  mapFinished = true;
  deleteV2K2 = autoDeleteV2K2;
  
  // ================== Log
  struct timeval logBegin , logEnd;
  initializeLog(logBegin);
  // ==================
  
  
  // +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  
  // ================== mutex
  logMutex = PTHREAD_MUTEX_INITIALIZER;
  mapMutex = PTHREAD_MUTEX_INITIALIZER;
  shuffleMutex = PTHREAD_MUTEX_INITIALIZER;
  reduceMutex = PTHREAD_MUTEX_INITIALIZER;
  // ==================
  
  threads.reserve((unsigned long)sizeThreads);
  
  
  for(unsigned int i = 0; i < sizeThreads ; ++i)
  {
    pthread_mutex_t threadMutex = PTHREAD_MUTEX_INITIALIZER;
    
    std::vector<MID_ITEM> threadVec;
    
    int res = pthread_create(&threads[i], NULL, &execMap, NULL);
    checkSysCall(res, "pthread_create");
    
    // mapContainerMap[i].push_back(threadVec);
    mapContainerMutex.push_back(threadMutex);
  
  }
  
  pthread_t shuffThread;
  int shuffRes = pthread_create(&shuffThread, NULL, &shuffle, NULL);
  checkSysCall(shuffRes, "pthread_create");
  
  for(unsigned int i = 0; i < sizeThreads ; ++i)
  {
    int res = pthread_join(threads[i], NULL);
    checkSysCall(res, "pthread_join");
  }
  
  mapFinished = false;
  sem_post(&semaphore);
  
  shuffRes = pthread_join(shuffThread, NULL);
  checkSysCall(shuffRes, "pthread_join");
  
  
  gettimeofday(&logEnd, NULL);
  long long mapShuffleElapsedTime = GET_ELAPSED_TIME;
  
  // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  
  
  threads.clear();
  threads.reserve((unsigned long)sizeThreads);
  position = 0;
  fillkey2vector();
  sizeInItemVector = prepareToReduce.size();
  
  gettimeofday(&logBegin, NULL);
  
  for (unsigned int j = 0; j < sizeThreads; ++j)
  {
    int res = pthread_create(&threads[j], NULL, &execReduce, NULL);
    checkSysCall(res, "pthread_create");
  }
  
  for (unsigned int k = 0; k < sizeThreads; ++k)
  {
    int res = pthread_join(threads[k], NULL);
    checkSysCall(res, "pthread_join");
  }
  
  mergeReduce();
  std::sort(mergeReduceContainer.begin(), mergeReduceContainer.end(), pairCompare);
  
  gettimeofday(&logEnd, NULL);
  long long reduceElapsedTime = GET_ELAPSED_TIME;
  ofs << "Map and Shuffle took " << mapShuffleElapsedTime<< " ns\n";
  ofs << "Reduce took " << reduceElapsedTime << " ns\n";
  
  destroyAll();
  threads.clear();
  
  ofs << "runMapReduceFramework finished\n";
  ofs.close();
  
  return mergeReduceContainer;

}
