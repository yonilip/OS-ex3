

#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H
#include "MapReduceClient.h"
#include <iostream>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <vector>
#include <utility>
#include <cygwin/types.h>
#include <iostream>
#include <bits/stl_map.h>
#include <stdlib.h>
#include <mutex>
#include <unordered_map>
#include <pthread.h>
#include <iterator>
#include <map>
#include <iterator>
#include <set>



typedef std::pair<k1Base*, v1Base*> IN_ITEM;
typedef std::pair<k3Base*, v3Base*> OUT_ITEM;
typedef std::pair<k2Base*, v2Base*> MID_ITEM;
typedef std::pair<std::vector<MID_ITEM>*,pthread_mutex_t*> THREAD_VALS;

typedef std::list<IN_ITEM> IN_ITEMS_LIST;
typedef std::list<OUT_ITEM> OUT_ITEMS_LIST;


/**
 * add comparator to initial unordered map so we can compare pthread keys
 */
struct pthreadCmp{
    bool operator()(const pthread_t a, const pthread_t b) const
    {
        int res = pthread_equal(a, b);
        if(res)
        {
            return false;
        }
        return true;
    }
};


// typedef of unordered map with costume comperator
typedef std::unordered_map<pthread_t, THREAD_VALS, std::hash<pthread_t>, pthreadCmp> THREAD_MAP;



OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_LIST& itemsList, int multiThreadLevel);

void Emit2 (k2Base*, v2Base*);
void Emit3 (k3Base*, v3Base*);

#endif //MAPREDUCEFRAMEWORK_H