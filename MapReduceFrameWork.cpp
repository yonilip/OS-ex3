/**
 * first we need to create thread pool for the given number of thread,
 * so we wont create mmore threads while program is runnunig.
 *
 * all thread will be stored in data structure and every time we would like to
 * send a task to a treahd we will search for a free one
 */
// first we need to create thread pool for the given number of thread,
//

#include "MapReduceFrameWork.h"
#include <sys/time.h>
#include <iostream>

#include <vector>
#include <map>
//#include <unordered_map>
#include <deque>
//#include <algorithm>

#define CHUNK 10


typedef std::pair<k2Base*, v2Base*> MID_ITEM;

//global variables:

/**
 * num of threads
 */
int threadLevel;

/**
 * instance of client map and reduce object functions
 */
MapReduceBase* mapBase;

/**
 * pointer to the cur place in inputVec
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

pthread_mutex_t inputListIterMutex;

pthread_mutex_t shuffledIterMutex;

/**
 * pointer to itemsList
 */
IN_ITEMS_LIST* itemsListGlobal;

IN_ITEMS_LIST::iterator iterEnd;

thread_local int tid;

int threadCount;
pthread_mutex_t threadCountMutex;


std::deque<std::pair<std::deque<MID_ITEM>, pthread_mutex_t>> globalMapVecContainers;

std::deque<std::deque<OUT_ITEM>> globalReduceContainers;

std::map<k2Base*, std::list<v2Base*>> shuffleMap;

bool keepShuffle;

std::map<k2Base*, std::list<v2Base*>>::iterator globalShuffledIter;

OUT_ITEMS_LIST mappedAndReducedList;

/**
 *	push to the threads deque the pair k2 v2
 */
void Emit2(k2Base* key, v2Base* val)
{
	pthread_mutex_lock(&globalMapVecContainers[tid].second);

	globalMapVecContainers[tid].first.push_back(std::make_pair(key, val));

	pthread_mutex_unlock(&globalMapVecContainers[tid].second);
}

/**
 *
 */
void Emit3 (k3Base* key, v3Base* val)
{
	globalReduceContainers[tid].push_back(std::make_pair(key, val));
}


void checkSysCall(int res)
{
	//TODO maybe not zero instead of neg
	if (res < 0)
	{
		std::cout << "error" << std::endl;
		exit(1);
	}
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
			// copy the pointers from the globalMapVecContainers at i and remove them
			pthread_mutex_lock(&globalMapVecContainers[i].second);

			MID_ITEM &frontPair = globalMapVecContainers[i].first.front(); //TODO maybe we need to pull a pointer?
/*			std::map<k2Base*, std::list<v2Base*>>::iterator it = shuffleMap.find(frontPair.first);
			if(it != shuffleMap.end())
			{
				it->second.push_back(frontPair.second);
			}
			else
			{
				std::list<v2Base*> list;
				list.push_back(frontPair.second);
				shuffleMap.insert(std::pair<k2Base*, std::list<v2Base*>>(frontPair.first, list));
			}*/
			shuffleMap[frontPair.first].push_back(frontPair.second);
			globalMapVecContainers[i].first.pop_front();

			pthread_mutex_unlock(&globalMapVecContainers[i].second);
		}
	}
}


/**
 * wait for signal, then go over each threads container and if not empty then
 * append the data from the pair into the shuffledData
 */
void *shuffle(void*)
{
	int res;
	struct timespec timeToWait;
	struct timeval now;

	// get absolute current time
	gettimeofday(&now, NULL);

	while(keepShuffle)
	{
		timeToWait.tv_sec = now.tv_sec;

		// usec is microsec, nsec is in nanosec
		timeToWait.tv_nsec = (now.tv_usec * 1000) + 10000000;

		res = pthread_cond_timedwait(&conditionVar, &timerMutex, &timeToWait);

		if(res == EINVAL || res == EPERM)
		{
			checkSysCall(-1);
		}

		pullDataFromMapping();
	}
	pullDataFromMapping(); //last iteration after execMap's are done
	pthread_exit(NULL);
}

/**
 * advance iterator lowerBound in safe manner such that wont exceed the end of
 * the container
 */
template <class T>
void safeAdvance(T& iter, const T& end)
{
	/*//size_t remaining((size_t) distance(iter, end));
	size_t remaining = ((size_t) std::distance(iter, end));
	int n = CHUNK;
	if (remaining < n)
	{
		n = (int) remaining;
	}
	std::advance(iter, n);*/
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
	pthread_mutex_lock(&threadCountMutex);
	tid = threadCount++;
	pthread_mutex_unlock(&threadCountMutex);

	/*pthread_mutex_t threadMutex = PTHREAD_MUTEX_INITIALIZER; //TODO destroy this
	std::deque<MID_ITEM> threadVec; //TODO check that shuffle wont try to access dast b4 first emit
	globalMapVecContainers[tid] = std::make_pair(threadVec, threadMutex);*/

	std::list<IN_ITEM>::iterator lowerBound, upperBound;

	while (itemListIter != iterEnd)
	{
		// lock itemListIter of inputVec, increase itemListIter by CHUNK.
		pthread_mutex_lock(&inputListIterMutex);
		lowerBound = itemListIter;
		//std::cout << "iter b4 adv: " << itemListIter
		safeAdvance(itemListIter, iterEnd);
		pthread_mutex_unlock(&inputListIterMutex);

		upperBound = lowerBound;
		safeAdvance(upperBound, iterEnd);

		for( ; lowerBound != upperBound; ++lowerBound)
		{
			mapBase->Map((*lowerBound).first, (*lowerBound).second);
		}
		// notify shuffle thread that there is un-empty container
		pthread_cond_signal(&conditionVar);
	}

	//after all chunks have been mapped
	pthread_exit(NULL);
}


void* execReduce(void*)
{
	pthread_mutex_lock(&threadCountMutex);
	tid = threadCount++;
	pthread_mutex_unlock(&threadCountMutex);

	std::map<k2Base*, std::list<v2Base*>>::iterator lowerBound, upperBound;

	while (globalShuffledIter != shuffleMap.end())
	{
		pthread_mutex_lock(&shuffledIterMutex);
		lowerBound = globalShuffledIter;
		safeAdvance(globalShuffledIter, shuffleMap.end());
		pthread_mutex_unlock(&shuffledIterMutex);

		upperBound = lowerBound;
		safeAdvance(upperBound, shuffleMap.end());

		for (; lowerBound != upperBound; ++lowerBound)
		{
			mapBase->Reduce((*lowerBound).first, (*lowerBound).second);
		}
	}
	pthread_exit(NULL);
}


/**
 * initial all global variacle so when calling runMapReduceFramework multiple
 * time will start new session
 */
void initializer()
{
	timerMutex = PTHREAD_MUTEX_INITIALIZER;
	conditionVar = PTHREAD_COND_INITIALIZER;
	threadCountMutex = PTHREAD_MUTEX_INITIALIZER;
	threadCount = 0;
	iterEnd = (*itemsListGlobal).end();
	itemListIter = (*itemsListGlobal).begin();
	std::cout << "input list size: " << (*itemsListGlobal).size() << std::endl; //TODO del
	keepShuffle = true;
}

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

/**std::deque<OUT_ITEM>
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
	//TODO should we make this more elegant?
	int res = pthread_mutex_destroy(&timerMutex);
	checkSysCall(res);
	res = pthread_mutex_destroy(&threadCountMutex);
	checkSysCall(res);
	res = pthread_mutex_destroy(&shuffledIterMutex);
	checkSysCall(res);
	res = pthread_mutex_destroy(&inputListIterMutex);
	checkSysCall(res);

	res = pthread_cond_destroy(&conditionVar);
	checkSysCall(res);
}

/**
 *
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


	// call initializer first so we can run runMapReduceFramework multiple time;
	initializer();

	// ***** SECOND PART: CREATING ALL EXECMAP THREADS *****
	std::vector<pthread_t> threads((unsigned long) threadLevel);

	// create all execMap threads
	for(int i = 0; i < threadLevel ; ++i) //TODO is 2nd eval needed?
	{
		pthread_mutex_t threadMutex = PTHREAD_MUTEX_INITIALIZER; //TODO destroy this
		std::deque<MID_ITEM> threadVec; //TODO check that shuffle wont try to access dast b4 first emit
		//globalMapVecContainers[tid] = std::make_pair(threadVec, threadMutex);
		globalMapVecContainers.push_back(std::make_pair(threadVec,
														threadMutex));

		int res = pthread_create(&threads[i], NULL, &execMap, NULL);
		checkSysCall(res);
	}

	pthread_t shuffThread;
	int shuffRes = pthread_create(&shuffThread, NULL, &shuffle, NULL);
	checkSysCall(shuffRes);

	// ***** THIRD PART: ADD SHUFFLE THREAD AND JOIN ALL THREADS *****
	//join the execMap
	for (int i = 0; i < threadLevel ; ++i)
	{
		int res = pthread_join(threads[i], NULL);
		checkSysCall(res);
	}

	//check that shuffle is done
	keepShuffle = false;
	shuffRes = pthread_join(shuffThread, NULL);
	checkSysCall(shuffRes);

	//reset threads vector and id's
	threads.clear();
	threads.reserve((unsigned long) threadLevel);
	threadCount = 0;

	std::cout << "Shuffle map size: " << shuffleMap.size() << std::endl;
	globalShuffledIter = shuffleMap.begin();

	//start Reduce
	for (int j = 0; j < threadLevel; ++j)
	{
		std::deque<OUT_ITEM> curDeque;
		globalReduceContainers.push_back(curDeque);
		int res = pthread_create(&threads[j], NULL, &execReduce, NULL);
		checkSysCall(res);
	}

	for (int k = 0; k < threadLevel; ++k)
	{
		int res = pthread_join(threads[k], NULL);
		checkSysCall(res);
	}

	mergeReducedContainers();
	mappedAndReducedList.sort(pairCompare);
	//std::sort(mappedAndReducedList.begin(), mappedAndReducedList.end(),
	//		  pairCompare);

	destroyMutexAndCond();

	return mappedAndReducedList;
}
