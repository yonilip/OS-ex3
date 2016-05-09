
#include "MapReduceFramework.h"
#include <sys/time.h>
#include <iostream>
#include <vector>
#include <map>
#include <deque>
#include <fstream>

#define CHUNK 10
#define SEC_TO_MICRO 1000000
#define MICRO_TO_NANO 1000

#define GET_ELAPSED_TIME ( ((logEnd.tv_sec - logBegin.tv_sec) * SEC_TO_MICRO) \
 						+(logEnd.tv_usec - logBegin.tv_usec) ) * MICRO_TO_NANO;

typedef std::pair<k2Base*, v2Base*> MID_ITEM;

/**
 * comparator for shuffle map keys, compare values of pointers
 */
struct shuffleComp{
	bool operator()(const k2Base* first, const k2Base* second)
	{
		return (*first)<(*second);
	}
};

//*******************global variables***********************:
/**
 * num of threads
 */
int threadLevel;

/**
 * instance of client map and reduce object functions
 */
MapReduceBase* mapBase;

/**
 * pointer to the cur place in inputList
 */
std::list<IN_ITEM>::iterator itemListIter;

/**
 * condition for notify shuffle
 */
pthread_cond_t conditionVar;

/**
 * mutex for timeout
 */
pthread_mutex_t timerMutex;

/**
 * mutex for input list iterator
 */
pthread_mutex_t inputListIterMutex;

/**
 * nutex for suffle list iterator
 */
pthread_mutex_t shuffledIterMutex;

/**
 * pointer to itemsList
 */
IN_ITEMS_LIST* itemsListGlobal;

/**
 * pointer to the end of itemList
 */
IN_ITEMS_LIST::iterator iterEnd;

/**
 * indicates which thread runs now, will change every time contex switch
 * applies according to the number matching to the thread.
 */
thread_local int tid;

/**
 * count to number of threads in order to give each of them a different
 * unique number
 */
int threadCount;

/**
 * mutex for threadCount
 */
pthread_mutex_t threadCountMutex;

/**
 * contains execMap containers and matching mutex to each container
 */
std::deque<std::pair<std::deque<MID_ITEM>, pthread_mutex_t>>
		globalMapVecContainers;

/**
 * queue of queques, contains all the containers for exec reduce threads
 */
std::deque<std::deque<OUT_ITEM>> globalReduceContainers;

/**
 * map k2Base keys to this list of valures after values being mapped by
 * execMap threads
 */
std::map<k2Base*, std::list<v2Base*>, shuffleComp> shuffleMap;

/**
 * indicated if shuffle continues
 */
bool keepShuffle;

/**
 * iterator for shuffleMap
 */
std::map<k2Base*, std::list<v2Base*>>::iterator globalShuffledIter;

/**
 * output list
 */
OUT_ITEMS_LIST mappedAndReducedList;

/**
 * stream for writing to log
 */
std::ofstream ofs;


//******************* methods  ***********************:


/**
 * check if return value of system call is invalid, print error to stderr
 */
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
void checkSysCall2(int res, std::string funcName)
{
	if (res != 0)
	{
		std::cout << "MapReduceFramework Failure: " << funcName
		<< " failed." << std::endl;
		exit(1);
	}
}

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
	ofs << "Thread " << ThreadName << " created [" <<
			getCurrentTime() << "]\n";
}

/**
 * print time of thread for log
 */
void logTerminateThread(std::string ThreadName)
{
	ofs << "Thread " << ThreadName << " terminated ["
	<< getCurrentTime() << "]\n";
}

/**
 *	push to the threads deque the pair k2 v2
 */
void Emit2(k2Base* key, v2Base* val)
{

	int lockRes = pthread_mutex_lock(&globalMapVecContainers[tid].second);
	checkSysCall2(lockRes, "pthread_mutex_lock");
	globalMapVecContainers[tid].first.push_back(std::make_pair(key, val));

	int unlockRes = pthread_mutex_unlock(&globalMapVecContainers[tid].second);
	checkSysCall2(unlockRes, "pthread_mutex_lock");
}

/**
 * add pair of k3Base and v3Base to matcing container of execReduce thread
 */
void Emit3 (k3Base* key, v3Base* val)
{
	globalReduceContainers[tid].push_back(std::make_pair(key, val));
}

/**
 * go over each threads container and if not empty then
 * append the data from the pair into the shuffledData
 */
void pullDataFromMapping()
{
	for (int i = 0; i < (int) globalMapVecContainers.size(); ++i)
	{
		while (!globalMapVecContainers[i].first.empty())
		{
			// copy the pointers from the globalMapVecContainers at i and
			// remove them
			int lockRes =
					pthread_mutex_lock(&globalMapVecContainers[i].second);
			checkSysCall2(lockRes, "pthread_mutex_lock");

			MID_ITEM& frontPair = globalMapVecContainers[i].first.front();

			shuffleMap[frontPair.first].push_back(frontPair.second);

			globalMapVecContainers[i].first.pop_front();

			int unlockRes =
					pthread_mutex_unlock(&globalMapVecContainers[i].second);
			checkSysCall2(unlockRes, "pthread_mutex_unlock");
		}
	}
}

/**
 * wait for signal, then go over each threads container and if not empty then
 * append the data from the pair into the shuffledData
 */
void *shuffle(void*)
{
	//print create to log
	int lockRes = pthread_mutex_lock(&threadCountMutex);
	checkSysCall2(lockRes, "pthread_mutex_lock");

	logCreateThread("Shuffle");

	int unlockRes = pthread_mutex_unlock(&threadCountMutex);
	checkSysCall2(unlockRes, "pthread_mutex_unlock");

	//times for condition
	int res;
	struct timespec timeToWait;
	struct timeval now;

	// get absolute current time
	checkSysCall(gettimeofday(&now, NULL), "gettimeofday");

	while(keepShuffle)
	{
		timeToWait.tv_sec = now.tv_sec;

		// usec is microsec, nsec is in nanosec
		timeToWait.tv_nsec = (now.tv_usec * 1000) + 10000000;

		res = pthread_cond_timedwait(&conditionVar, &timerMutex, &timeToWait);

		if(res == EINVAL || res == EPERM)
		{
			checkSysCall(-1, "pthread_cond_timedwait");
		}

		pullDataFromMapping();
	}
	pullDataFromMapping(); //last iteration after execMap's are done


	//print termination to log
	lockRes = pthread_mutex_lock(&threadCountMutex);
	checkSysCall2(lockRes, "pthread_mutex_lock");

	logTerminateThread("Shuffle");

	unlockRes = pthread_mutex_unlock(&threadCountMutex);
	checkSysCall2(unlockRes, "pthread_mutex_unlock");

	pthread_exit(NULL);
}

/**
 * advance iterator lowerBound in safe manner such that wont exceed the end of
 * the container
 */
template <class T>
void safeAdvance(T& iter, const T& end)
{
	for (int i = 0; i < CHUNK; ++i)
	{
		if (iter == end)
			return;
		++iter;
	}
}

/**
 * Execution of Map() for each pThread
 * locks the global upper index of the input Vec and assigns lower and upper
 * bounds and update the global itemListIter += CHUNK
 * for each index in the bound activate map()
 * when done notify the conditional shuffle thread
 */
void* execMap(void*)
{
	int lockRes = pthread_mutex_lock(&threadCountMutex);
	checkSysCall2(lockRes, "pthread_mutex_lock");

	tid = threadCount++;
	logCreateThread("ExecMap");

	int unlockRes = pthread_mutex_unlock(&threadCountMutex);
	checkSysCall2(unlockRes, "pthread_mutex_unlock");

	std::list<IN_ITEM>::iterator lowerBound, upperBound;

	while (itemListIter != iterEnd)
	{
		// lock itemListIter of inputVec, increase itemListIter by CHUNK.
		lockRes = pthread_mutex_lock(&inputListIterMutex);
		checkSysCall2(lockRes, "pthread_mutex_lock");
		lowerBound = itemListIter;
		safeAdvance(itemListIter, iterEnd);
		checkSysCall2(pthread_mutex_unlock(&inputListIterMutex),
					  "pthread_mutex_unlock");

		upperBound = lowerBound;
		safeAdvance(upperBound, iterEnd);

		for( ; lowerBound != upperBound; ++lowerBound)
		{
			mapBase->Map((*lowerBound).first, (*lowerBound).second);
		}
		// notify shuffle thread that there is un-empty container
		unlockRes = pthread_cond_signal(&conditionVar);
		checkSysCall2(unlockRes, "pthread_mutex_unlock");
	}

	//after all chunks have been mapped

	//print termination to log
	lockRes = pthread_mutex_lock(&threadCountMutex);
	checkSysCall2(lockRes, "pthread_mutex_lock");

	logTerminateThread("ExecMap");

	unlockRes = pthread_mutex_unlock(&threadCountMutex);
	checkSysCall2(unlockRes, "pthread_mutex_unlock");


	pthread_exit(NULL);
}

/**
 * Execution of Reduce() for each pThread
 * locks the global upper index of the suffleMap and assigns lower and upper
 * bounds and update the global suffleMap
 * for each index in the bound activate reduce()
 */
void* execReduce(void*)
{
	int lockRes = pthread_mutex_lock(&threadCountMutex);
	checkSysCall2(lockRes, "pthread_mutex_lock");

	tid = threadCount++;
	logCreateThread("ExecReduce");

	int unlockRes = pthread_mutex_unlock(&threadCountMutex);
	checkSysCall2(unlockRes, "pthread_mutex_unlock");

	std::map<k2Base*, std::list<v2Base*>>::iterator lowerBound, upperBound;

	while (globalShuffledIter != shuffleMap.end())
	{
		lockRes = pthread_mutex_lock(&shuffledIterMutex);
		checkSysCall2(lockRes, "pthread_mutex_lock");
		lowerBound = globalShuffledIter;
		safeAdvance(globalShuffledIter, shuffleMap.end());
		unlockRes = pthread_mutex_unlock(&shuffledIterMutex);
		checkSysCall2(unlockRes, "pthread_mutex_unlock");

		upperBound = lowerBound;
		safeAdvance(upperBound, shuffleMap.end());

		for (; lowerBound != upperBound; ++lowerBound)
		{
			mapBase->Reduce((*lowerBound).first, (*lowerBound).second);

		}
	}

	//print termination to log
	lockRes = pthread_mutex_lock(&threadCountMutex);
	checkSysCall2(lockRes, "pthread_mutex_lock");

	logTerminateThread("ExecReduce");

	unlockRes = pthread_mutex_unlock(&threadCountMutex);
	checkSysCall2(unlockRes, "pthread_mutex_unlock");

	pthread_exit(NULL);
}


/**
 * initial all global variables so when calling runMapReduceFramework multiple
 * times it will start a new session
 */
void initializer()
{
	timerMutex = PTHREAD_MUTEX_INITIALIZER;
	conditionVar = PTHREAD_COND_INITIALIZER;
	threadCountMutex = PTHREAD_MUTEX_INITIALIZER;
	inputListIterMutex = PTHREAD_MUTEX_INITIALIZER;
	shuffledIterMutex = PTHREAD_MUTEX_INITIALIZER;
	threadCount = 0;
	iterEnd = (*itemsListGlobal).end();
	itemListIter = (*itemsListGlobal).begin();
	keepShuffle = true;
	if (!mappedAndReducedList.empty())
	{
		mappedAndReducedList.clear();
	}
}

/**
 * merges all the containers that were made in the reduce process
 */
void mergeReducedContainers()
{
	for (int i = 0; i < (int) globalReduceContainers.size(); ++i)
	{
		while (!globalReduceContainers[i].empty())
		{
			OUT_ITEM item = globalReduceContainers[i].front();
			mappedAndReducedList.push_back(item);
			globalReduceContainers[i].pop_front();
		}
	}
}

/**
 * compare between keys of OUT_ITEM pairs
 */
bool pairCompare(const OUT_ITEM& left, const OUT_ITEM& right)
{
	return (*left.first) < (*right.first);
}

/**
 * destroy pthread mutex and conditional objects
 */
void destroyMutexAndCond()
{
	int res = pthread_mutex_destroy(&timerMutex);
	checkSysCall(res, "pthread_mutex_destroy");
	res = pthread_mutex_destroy(&threadCountMutex);
	checkSysCall(res, "pthread_mutex_destroy");
	res = pthread_mutex_destroy(&shuffledIterMutex);
	checkSysCall(res, "pthread_mutex_destroy");
	res = pthread_mutex_destroy(&inputListIterMutex);
	checkSysCall(res, "pthread_mutex_destroy");
	res = pthread_cond_destroy(&conditionVar);
	checkSysCall(res, "pthread_mutex_destroy");
}

/**
 * terminates the process,release all containers and destroy mutex and
 * conditions
 */
void prepareForEndOfFramework()
{
	destroyMutexAndCond();
	for (int i = 0; i < (int) globalMapVecContainers.size(); ++i)
	{
		globalMapVecContainers[i].first.clear();
		int res = pthread_mutex_destroy(&globalMapVecContainers[i].second);
		checkSysCall(res, "pthread_mutex_destroy");
	}
	globalMapVecContainers.clear();
	for (int j = 0; j < (int) globalReduceContainers.size(); ++j)
	{
		globalReduceContainers[j].clear();
	}
	globalReduceContainers.clear();
	std::map<k2Base *, std::list<v2Base *>, shuffleComp>::iterator
			it = shuffleMap.begin();
	for ( ; it != shuffleMap.end() ; ++it)
	{
		it->second.clear();
	}
	shuffleMap.clear();
}

/**
 * init log
 */
void initializeLog(timeval& begin)
{
	std::string currentWorkDir(__FILE__);
	std::string logPath =
			currentWorkDir.substr(0, currentWorkDir.find_last_of("\\/") + 1)
			+ ".MapReduceFramework.log";

	ofs.open(logPath, std::ofstream::app);

	ofs << "runMapReduceFramework started with " << threadLevel
	<<  " threads\n";

	gettimeofday(&begin, NULL);
}

/**
 * run mapReduceFramework for given MapReduceBase instance implemented by the
 * client
 * get the given <k1,v1> input list and form <k3,v3> output list by
 * parallelizing map and reduce action between a guven multiThreadLevel
 * amount of threads in a optimize time
 */
OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase &mapReduce,
									IN_ITEMS_LIST &itemsList,
									int multiThreadLevel)
{
	// ***** FIRST PART: INIT ALL VALUES: *****
	// initial all global variables according to input
	mapBase = &mapReduce;
	threadLevel = multiThreadLevel;
	itemsListGlobal = &itemsList;

	struct timeval logBegin , logEnd;
	initializeLog(logBegin);

	// call initializer first so we can run runMapReduceFramework multiple time
	initializer();

	// ***** SECOND PART: CREATING ALL EXECMAP THREADS *****
	std::vector<pthread_t> threads((unsigned long) threadLevel);

	// create all execMap threads
	for(int i = 0; i < threadLevel ; ++i)
	{
		pthread_mutex_t threadMutex = PTHREAD_MUTEX_INITIALIZER;
		std::deque<MID_ITEM> threadVec;
		globalMapVecContainers.push_back(std::make_pair(threadVec,
														threadMutex));

		int res = pthread_create(&threads[i], NULL, &execMap, NULL);
		checkSysCall(res, "pthread_create");
	}

	pthread_t shuffThread;
	int shuffRes = pthread_create(&shuffThread, NULL, &shuffle, NULL);
	checkSysCall(shuffRes,  "pthread_create");

	// ***** THIRD PART: ADD SHUFFLE THREAD AND JOIN ALL THREADS *****
	//join the execMap
	for (int i = 0; i < threadLevel ; ++i)
	{
		int res = pthread_join(threads[i], NULL);
		checkSysCall(res,  "pthread_join");
	}

	//check that shuffle is done
	keepShuffle = false;
	shuffRes = pthread_join(shuffThread, NULL);
	checkSysCall(shuffRes, "pthread_join");

	gettimeofday(&logEnd, NULL);
	long long mapShuffleElapsedTime = GET_ELAPSED_TIME;

	//reset threads vector and id's
	threads.clear();
	threads.reserve((unsigned long) threadLevel);
	threadCount = 0;
	globalShuffledIter = shuffleMap.begin();

	gettimeofday(&logBegin, NULL);

	//start Reduce
	for (int j = 0; j < threadLevel; ++j)
	{
		std::deque<OUT_ITEM> curDeque;
		globalReduceContainers.push_back(curDeque);
		int res = pthread_create(&threads[j], NULL, &execReduce, NULL);
		checkSysCall(res, "pthread_create");
	}

	for (int k = 0; k < threadLevel; ++k)
	{
		int res = pthread_join(threads[k], NULL);
		checkSysCall(res, "pthread_join");
	}

	mergeReducedContainers();
	mappedAndReducedList.sort(pairCompare);

	gettimeofday(&logEnd, NULL);
	long long reduceElapsedTime = GET_ELAPSED_TIME;
	ofs << "Map and Shuffle took " << mapShuffleElapsedTime<< " ns\n";
	ofs << "Reduce took " << reduceElapsedTime << " ns\n";
	
	threads.clear();
	prepareForEndOfFramework();

	ofs << "runMapReduceFramework finished\n";
	ofs.close();

	return mappedAndReducedList;
}