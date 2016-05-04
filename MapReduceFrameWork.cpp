/**
 * first we need to create thread pool for the given number of thread,
 * so we wont create mmore threads while program is runnunig.
 *
 * all thread will be stored in data structure and every time we would like to
 * send a task to a treahd we will search for a free one
 */
// first we need to create thread pool for the given number of thread,
//

#include <sys/time.h>
#include "MapReduceFrameWork.h"
#include <iostream>

#include <vector>
#include <map>
#include <unordered_map>
#include <list>
#include <deque>

#define SUCCEESS 0
#define CHUNK 10


typedef std::pair<k2Base*, v2Base*> MID_ITEM;



//TODO redo includes and make clean h file
using namespace std;


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
list<IN_ITEM>::iterator itemListIter;

/**
 * condition for notify shuffle
 */
pthread_cond_t conditionVar;

/**
 * mutex for timeout
 */
pthread_mutex_t timerMutex;

pthread_mutex_t* inputListIterMutex;

/**
 * pointer to itemsList
 */
IN_ITEMS_LIST* itemsListGlobal;

auto iterEnd = (*itemsListGlobal).end();

thread_local int tid;

int threadCount;
pthread_mutex_t threadCountMutex;


deque<pair<deque<MID_ITEM>, pthread_mutex_t>> globalVecContainers;

map<k2Base*, deque<v2Base*>> shuffleMap;

bool keepShuffle;


/**
 *	push to the threads deque the pair k2 v2
 */
void Emit2(k2Base* key, v2Base* val)
{
	pthread_mutex_lock(&globalVecContainers[tid].second);

	globalVecContainers[tid].first.push_back(make_pair(key, val));

	pthread_mutex_unlock(&globalVecContainers[tid].second);
}

/**
 *
 */
void Emit3 (k3Base*, v3Base*)
{

}


void checkSysCall(int res)
{
	//TODO maybe not zero instead of neg
	if (res < 0)
	{
		cout << "error" << endl;
		exit(1);
	}
}

/**
 * go over each threads container and if not empty then
 * append the data from the pair into the shuffledData
 */
void pullDataFromMapping()
{
	for (int i = 0; i < (int) globalVecContainers.size(); ++i)
	{
		if (!globalVecContainers[i].first.empty())
		{
			pthread_mutex_lock(&globalVecContainers[i].second);
			while (!globalVecContainers[i].first.empty())
			{
				// copy the pointers from the globalVecContainers at i and remove them
				MID_ITEM &frontPair = globalVecContainers[i].first.front();
				shuffleMap[frontPair.first].push_back(frontPair.second); //TODO check that this inits a deque as the value on the first time
				globalVecContainers[i].first.pop_front();
			}
			pthread_mutex_unlock(&globalVecContainers[i].second);
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
void safeAdvance(list<IN_ITEM>::iterator& iter)
{
	size_t remaining((size_t) distance(iter, iterEnd));
	int n = CHUNK;
	if (remaining < CHUNK)
	{
		n = (int) remaining;
	}
	advance(iter, n);
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

	pthread_mutex_t threadMutex = PTHREAD_MUTEX_INITIALIZER;
	vector<MID_ITEM> threadVec;
	globalVecContainers[tid] = make_pair(threadVec, threadMutex);


	list<IN_ITEM>::iterator lowerBound, upperBound;

	while (itemListIter != iterEnd)
	{
		// lock itemListIter of inputVec, increase itemListIter by CHUNK.
		pthread_mutex_lock(inputListIterMutex);
		lowerBound = itemListIter;
		safeAdvance(itemListIter);
		pthread_mutex_unlock(inputListIterMutex);

		upperBound = lowerBound;
		safeAdvance(upperBound);


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


	map<k2Base*, deque<v2Base*>>::iterator lowerBound, upperBound;




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
	itemListIter = (*itemsListGlobal).begin();
	keepShuffle = true;
}

/**
 *
 */
OUT_ITEMS_LIST runMapRedueFramework(MapReduceBase &mapReduce,
									IN_ITEMS_LIST &itemsList,
									int multiThreadLevel)
{

	//TODO start with init of shuffle
	pthread_t shuffThread;
	int shuffRes = pthread_create(&shuffThread, NULL, &shuffle, NULL);
	checkSysCall(shuffRes);


	// ***** FIRST PART: INIT ALL VALUES: *****

	// initial all global variables according to input
	mapBase = &mapReduce;
	threadLevel = multiThreadLevel;
	*itemsListGlobal = itemsList;


	// call initializer first so we can run runMapReduceFramework multiple time;
	initializer();

	// ***** SECOND PART: CREATING ALL EXECMAP THREADS *****
	vector<pthread_t> threads((unsigned long) threadLevel);

	// create all execMap threads
	for(int i = 0; i < threadLevel && itemListIter != iterEnd ; ++i) //TODO is 2nd eval needed?
	{
		int res = pthread_create(&threads[i], NULL, &execMap, NULL);
		checkSysCall(res);
	}

	// ***** THIRD PART: ADD SHUFFLE THREAD AND JOIN ALL THREADS *****
	//join the threads
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

	//start Reduce
	for (int j = 0; j < threadLevel; ++j)
	{
		int res = pthread_create(&threads[j], NULL, &execReduce, NULL);
		checkSysCall(res);
	}

	for (int k = 0; k < threadLevel; ++k)
	{
		int res = pthread_join(threads[k], NULL);
		checkSysCall(res);
	}




	//TODO use pthread_mutex_destroy(mutex) to free mutex objects

	//TODO use pthread_cond_destroy(cond) to free cond objects

}

int main()
{
	//TODO delete!! this is lib not exec
}
