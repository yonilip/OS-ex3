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
#define SUCCEESS 0
#define CHUNK 10


using namespace std;


//global variables:
int threadLevel;
MapReduceBase* mapBase;
vector<IN_ITEM> inpurVec;
int index;
pthread_mutex_t *iterMutex;
pthread_mutex_t *mapMutex;
THREAD_MAP threadsMap;
pthread_cond_t conditionVar;
pthread_mutex_t* timerMutex;
map<k2Base*, list<v2Base>*> shuffleMap;


/**
 *
 */
void Emit2 (k2Base* key, v2Base* val)
{

    int found = 0;
    pthread_t pid = pthread_self();
    THREAD_VALS threadVal;
    // lock map in order to search for the right thread (otherwise map can change while searching)
    pthread_mutex_lock(mapMutex);

    // search for pid in map
    for(auto it = threadsMap.begin(); it != threadsMap.end(); ++it)
    {
        if(pthread_equal(pid, (*it).first))
        {
            threadVal = threadsMap.at(pid);
            found = 1;
            break;

        }
    }
    pthread_mutex_unlock(mapMutex);

    // add pair only in case we found our thread in map
    //TODO check if we can assume it will always be there
    if(found)
    {
        pthread_mutex_lock(threadVal.second);
        threadVal.first->push_back(make_pair(key,val));
        pthread_mutex_unlock(threadVal.second);
    }

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
 *
 */
void *execMap(void*)
{
    // check what to do in case input reaches to end
    int currIndex = index;

    // lock index of inputVec, increase index by CHUNK.
    pthread_mutex_lock(iterMutex);
    index += CHUNK;
    pthread_mutex_unlock(iterMutex);

    // map all pairs in the range of this chunk. no other thread will map 
    // those values other then working thread
    for(currIndex; currIndex < index; ++currIndex)
    {
        mapBase->Map(inpurVec[currIndex].first, inpurVec[currIndex].second);
    }

    // notify shuffle thread that there is un-empty container
    pthread_cond_signal(&conditionVar);
}


/**
 * initial all global variacle so when calling runMapReduceFramework multiple
 * time will start new session
 */
void initializer()
{
    *iterMutex = PTHREAD_MUTEX_INITIALIZER;
    *mapMutex = PTHREAD_MUTEX_INITIALIZER;

    *timerMutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t conditionVar = PTHREAD_COND_INITIALIZER;
    threadsMap = THREAD_MAP();


    index = 0;
}


/**
 *
 */
OUT_ITEMS_LIST rcunMapRedueFramework(MapReduceBase& mapReduce, IN_ITEMS_LIST& itemsList, int multiThreadLevel)
{

    // ***** FIRST PART: INIT ALL VALUES: *****

    // initial all global variables according to input
    mapBase = &mapReduce;
    threadLevel = multiThreadLevel;
    inpurVec = {itemsList.begin(),itemsList.end()};


    // call initializer first so we can run runMapReduceFramework multiple time;
    initializer();

    // ***** SECOND PART: CREATING ALL EXECMAP THREADS *****

    pthread_t pid;
    pthread_mutex_t* threadMutex;
    int res;

    // create all execMap threads
    for(int i = 0; i < threadLevel ; ++i)
    {

        // check if there are more pairs in inputList
        if(index >= inpurVec.size())
        {
            break;
        }

        res = pthread_create(&pid, NULL, &execMap, NULL);

        // check if creations succeed
        if (res < 0)
        {
            cout << "error" << endl;
            exit(1);
        }

        *threadMutex = PTHREAD_MUTEX_INITIALIZER;
        vector<MID_ITEM> threadVec;

        // lock map while insert new thread (in case shuffle thread tries to search in map at the same time
        pthread_mutex_lock(mapMutex);
        threadsMap.insert(make_pair(pid, make_pair(&threadVec,threadMutex)));
        pthread_mutex_unlock(mapMutex);
    }

    // ***** THIRD PART: ADD SHUFFLE THREAD AND JOIN ALL THREADS *****


}