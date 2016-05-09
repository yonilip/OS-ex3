

#include <string>

#include "MapReduceFramework.h"
#include <iostream>
#include <fstream>
#include <string.h>
#include <sstream>
#include <dirent.h>
#include <stdlib.h>

#define THREAD_LEVEL 7

int multiThreadLevel = THREAD_LEVEL;

/** we get the substring as an argument to main, but map and reduce need
  this variable to work properly, the best solution i found was to make it
 global.. need to check it is legit
 **/
std::string subString;

void searchCheckSysCall(int res)
{
    if (res != 0)
    {
        std::cout << "error" << std::endl;
        exit(1);
    }
}


/**
 * inherits from k1 Base, holds directory name
 */
class DirNameKey : public k1Base
{
public:
    std::string dirName;
    DirNameKey(std::string dirName) : dirName(dirName) {};
    ~DirNameKey(){}

    bool operator<(const k1Base &other) const
    {
		return dirName < ((const DirNameKey&)other).dirName;
    }
};

class DirVal : public v1Base
{
public:
    int dirVal;
    DirVal() : dirVal(0){}
    ~DirVal(){}
};

/**
 * same implementation as DirNameKey
 */

class FileName2 : public k2Base
{
public:
    std::string fileName;
    ~FileName2(){}
    FileName2(std::string fileName) : fileName(fileName){};

    bool operator<(const k2Base &other) const
    {
		return fileName < ((const FileName2&)other).fileName;
    }
};

class FileName3 : public k3Base
{
public:
    std::string fileName;
    ~FileName3(){}
    FileName3(std::string fileName) : fileName(fileName){};

    bool operator<(const k3Base &other) const
    {
        return fileName < ((const FileName3&)other).fileName;
    }
};



class FileValue : public v2Base
{
public:
    int fileVal;

    FileValue() : fileVal(1){};
    ~FileValue(){}
};

class FileCount : public v3Base
{
public:
    int fileCount;

    FileCount(int count) : fileCount(count){};
    ~FileCount(){}
};

std::list<std::pair<FileName2*, FileValue*> > destroyContainerK2;

class SubStringMapReduce : public MapReduceBase
{
    void Map(const k1Base *const key, const v1Base *const val) const
    {
		DirNameKey* dir = ((DirNameKey *)key);
        std::string dirName = dir->dirName;
		(void)val;
        /**
         * the idea is to parse given directory given as key and add all file
         * in it to list using Emit function that was given to us
         */
		DIR* pDIR = opendir(dirName.c_str());
        if(pDIR)
        {
			struct dirent* entry = readdir(pDIR);

			do
            {
				//ignore "." and ".."
                if(strcmp(entry->d_name, ".") != 0 &&
						strcmp(entry->d_name, "..") != 0){
                    if (strstr(entry->d_name, subString.c_str()))
                    {
                        std::stringstream ss;
                        std::string s;
                        ss << entry->d_name;
                        ss >> s;
						FileName2* fileName = new FileName2(s);
                        FileValue* fileVal = new FileValue();
                        destroyContainerK2.push_back(std::make_pair(fileName,
                                                                    fileVal));
                        Emit2((k2Base*)fileName, (v2Base*)fileVal);
                        //TODO check where to delete
                    }
                }
				entry = readdir(pDIR);
            } while(entry) ;
			closedir(pDIR);
        }
    }

    void Reduce(const k2Base *const key, const V2_LIST &vals) const
    {
        FileCount* fileCount;
		FileName3* fileName = ((FileName3*)key);
        FileValue* fileVal;
        int counter = 0;
        for(v2Base* val : vals)
        {
			fileVal = ((FileValue*)val);
            counter += fileVal->fileVal;
        }
        fileCount = new FileCount(counter);
        Emit3((k3Base* )fileName, (v3Base*)fileCount);
        //TODO check where to delete
    }
};

int main(int argc, char* argv[])
{
    if (argc < 2) {
        std::cerr << "Usage: <substring to search> "
							 "<folders, separated by space>" << std::endl;
        return 1;
    }

    // get the substring need to check and store it in a global variable so map
    // and reduce can use it
    subString = argv[1];

    IN_ITEMS_LIST directories;
    OUT_ITEMS_LIST result;

    for(int i = 2; i < argc; i++)
    {
        std::string str = argv[i];
        DirNameKey* key = new DirNameKey(str);
        DirVal* val = new DirVal();
        directories.push_back(std::make_pair((k1Base*) key,(v1Base*) val));
    }

	SubStringMapReduce searchMapReduce;
    result = runMapReduceFramework(searchMapReduce, directories,
								   multiThreadLevel);

    for(OUT_ITEM item : result)
    {
		FileCount* fileCount = ((FileCount*)item.second);
        int count = fileCount->fileCount;
        for(int i = 0;i < count; ++i)
        {
			FileName3* file = ((FileName3*)item.first);
            std::cout << file->fileName << std::endl;
        }
    }

    for(IN_ITEM item1 : directories)
    {
       delete(item1.first);
       delete(item1.second);
    }
    directories.clear();
    for(OUT_ITEM item2 : result)
    {
        delete(item2.second);
    }
    result.clear();
    for (std::pair<FileName2*, FileValue*> item3 : destroyContainerK2)
    {
        if(item3.first != nullptr){
            delete(item3.first);
        }
        if (item3.second != nullptr)
        {
            delete(item3.second);
        }
    }
    destroyContainerK2.clear();
}