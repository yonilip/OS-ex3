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

#define SUCCEESS 0
#define CHUNK 10


/**
 * add comparator to initial unordered map so we can compare pthread keys
 */
struct pthreadCmp{
    bool operator()(const pthread_t a, const pthread_t b) const
    {
        int res = pthread_equal(a, b);
		return (res == 0);
	}
};


typedef std::pair<k2Base*, v2Base*> MID_ITEM;
typedef std::pair<std::vector<MID_ITEM>*, pthread_mutex_t*> THREAD_VALS;
typedef std::unordered_map<pthread_t, THREAD_VALS, std::hash<pthread_t>, pthreadCmp> THREAD_MAP;


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
 * initial given data
 */


/**
 * pointer to the cur place in inputVec
 */
list<IN_ITEM>::iterator itemListIter;

/**
 * mutex for threadMap
 */
pthread_mutex_t *mapMutex;

/**
 * map of pThreads
 */
THREAD_MAP threadsMap;

/**
 * condition for notify shuffle
 */
pthread_cond_t conditionVar;

/**
 * mutex for timeout
 */
pthread_mutex_t* timerMutex;

//pthread_mutex_t inputListIterMutex;
pthread_mutex_t*inputListIterMutex;

/**
 * output of shuffle function
 */
map<k2Base*, list<v2Base>*> shuffleMap;

/**
 * pointer to itemsList
 */
IN_ITEMS_LIST* itemsListGlobal;

auto iterEnd = (*itemsListGlobal).end();


/**
 *
 */
void Emit2(k2Base* key, v2Base* val)
{

    int found = 0;
    pthread_t pid = pthread_self();
    THREAD_VALS threadVal;
    // lock map in order to search for the right thread (otherwise map can change while searching)
    pthread_mutex_lock(mapMutex);

    // search for pid in map
//    for(auto it = threadsMap.begin(); it != threadsMap.end(); ++it)
//    {
//        if(pthread_equal(pid, (*it).first))
//        {
    threadVal = (*threadsMap.at(pid));
//            found = 1;
//            break;
//
//        }
//    }
    pthread_mutex_unlock(mapMutex);

    // add pair only in case we found our thread in map
    //TODO check if we can assume it will always be there
//    if(found)
//    {
    pthread_mutex_lock(threadVal.second);
    threadVal.first->push_back(make_pair(key,val));
    pthread_mutex_unlock(threadVal.second);
//    }

}

/**
 *
 */
void Emit3 (k3Base*, v3Base*)
{

}

void *shuffle(void*)
{
    int res;

    struct timespec timeToWait;
    struct timeval now;


    // get absolute current time
    gettimeofday(&now, NULL);

    while(true)
    {
        timeToWait.tv_sec = now.tv_sec;

        //TODO check if conversion is OK
        timeToWait.tv_nsec = now.tv_usec + 10000000;

        //TODO no need to lock this because there is only oe consumer
        //pthread_mutex_lock(timerMutex);
        res = pthread_cond_timedwait(&conditionVar, timerMutex, &timeToWait);
        //pthread_mutex_unlock(timerMutex);

        // in this case we got a signal that was sent by one of execMap threads.
        // we now that there is defiantly one container that is not empty (the one belongs to the thread that sent the signal)
        if(res == SUCCEESS)
        {
            // iterate threadMap until we find all containers that are nor empty
            for(auto it = threadsMap.begin(); it != threadsMap.end(); ++it)
            {
                THREAD_VALS vals = it->second;
                if(!vals.first->empty())
                {
                    pthread_mutex_lock(vals.second);
                    for(MID_ITEM pair : *vals.first)
                    {
                        k2Base* key = pair.first;
                        list<v2Base>* listPtr = shuffleMap[key];
                        listPtr->push_back(*pair.second);
                    }
                    // erase values from container
                    vals.first->clear();
                    pthread_mutex_unlock(vals.second);
                }
            }
            continue;
        }

        if(res == EINVAL || res == EPERM)
        {
            // exception??
            //TODO what sould we do here
            break;
        }

        if(res == ETIMEDOUT)
        {

        }
        break;

    }

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
void execMap(void*)
{
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
}


/**
 * initial all global variacle so when calling runMapReduceFramework multiple
 * time will start new session
 */
void initializer()
{
	*inputListIterMutex = PTHREAD_MUTEX_INITIALIZER;
    *mapMutex = PTHREAD_MUTEX_INITIALIZER;

    *timerMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t conditionVar = PTHREAD_COND_INITIALIZER;
    threadsMap = THREAD_MAP();


    itemListIter = (*itemsListGlobal).begin();
}


/**
 *
 */
OUT_ITEMS_LIST runMapRedueFramework(MapReduceBase &mapReduce,
									IN_ITEMS_LIST &itemsList,
									int multiThreadLevel)
{

    //TODO start with init of shuffle

    // ***** FIRST PART: INIT ALL VALUES: *****

    // initial all global variables according to input
    mapBase = &mapReduce;
    threadLevel = multiThreadLevel;
	*itemsListGlobal = itemsList; // TODO keep pointer and use list, update all dependant things to iters (int indices and such)


    // call initializer first so we can run runMapReduceFramework multiple time;
    initializer();

    // ***** SECOND PART: CREATING ALL EXECMAP THREADS *****


	// create all execMap threads
	for(int i = 0; i < threadLevel && itemListIter != iterEnd ; ++i)
    {
		pthread_t tid;
		pthread_mutex_t* threadMutex;

        // TODO do we need to lock list for this check?

        int res =
				pthread_create(&tid, NULL, (void *(*)(void *)) &execMap, NULL);

        // check if creations succeed
        if (res < 0)
        {
            cout << "error" << endl;
            exit(1);
        }

        *threadMutex = PTHREAD_MUTEX_INITIALIZER;
        vector<MID_ITEM> threadVec; //TODO this might be erased after scope finishes

        // lock map while insert new thread (in case shuffle thread tries to search in map at the same time
        pthread_mutex_lock(mapMutex);
        threadsMap.insert(make_pair(tid, make_pair(&threadVec, threadMutex)));
        pthread_mutex_unlock(mapMutex);
		//TODO might be good to make a waiter here until all is done so scope dosent kill vars and stuff
    }

    // ***** THIRD PART: ADD SHUFFLE THREAD AND JOIN ALL THREADS *****

}

int main()
{
    //TODO delete!! this is lib not exec
}
