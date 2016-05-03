

#include "MapReduceClient.h"
#include "MapReduceFrameWork.h"
#include <iostream>
#include <fstream>
#include<cstdlib>
#include<string.h>
#include<sstream>
#include<dirent.h>

#define THREAD_LEVEL 10

int multiThreadLevel = THREAD_LEVEL; //TODO get updated from given param

/** we get the substring as an argument to main, but map and reduce need
  this variable to work properly, the best solution i found was to make it
 global.. need to check it is legit
 **/
string subString;
using namespace std;


/**
 * inherits from k1 Base, holds directory name
 */
class DirNameKey : public k1Base
{
private:
    string dirName;
public:
    // TODO make sure constructor is needed
    DirNameKey(string dirName) : dirName(dirName){};

    string getDirName()
    {
        return dirName;
    }

    bool operator<(const k1Base &other) const
    {
        DirNameKey* otherStr = const_cast<DirNameKey*>(&other);
        return (dirName < otherStr->dirName);
    }
};

//TODO does it matter if its not null as the given example on the ex description?
class DirVal : public v1Base
{
private:
    int dirVal;
public:
    DirVal() : dirVal(0){}
};

/**
 * same implementation as DirNameKey
 */

//TODO check if multiple inheritence is allwed, does it get the wanted result in this case?
class FileName : public k2Base, public k3Base
{
private:
    string fileName;
public:
    FileName(string fileName) : fileName(fileName){};
    string getFileName()
    {
        return fileName;
    }
    bool operator<(const k2Base &other) const
    {
        FileName* otherStr = const_cast<FileName*>(&other);
        return (fileName < otherStr->fileName);
    }
};

class FileValue : public v2Base
{
private:
    int fileVal;
public:
    FileValue() : fileVal(1){};
    int getFileVal()
    {
        return fileVal;
    }
};

class FileCount : public v3Base
{
private:
    int fileCount;
public:
    FileCount(int count) : fileCount(count){};
    int getFileCount()
    {
        return fileCount;
    }
};


class SubStringMapReduce : public MapReduceBase
{
    void Map(const k1Base *const key, const v1Base *const val) const
    {

    /**
     * this snippet of code was taken from OS group in facebook, someone
     * posted a link for reading all files in given directory so i used it
     * but im not sure if it works :(
     */
        FileName* fileName;
        FileValue* fileVal;
        DirNameKey* dir = const_cast<DirNameKey*>(&key);
        string dirName = dir->getDirName();
        const char* cStr = dirName.c_str();
        ifstream inn;
        string str;
        DIR *pDIR;
        struct dirent *entry;

        /**
         * the idea is to parse given directory given as key and add all file
         * in it to list using Emit function that was given to us
         */
        if(pDIR = opendir(cStr))
        {
            while(entry = readdir(pDIR))
            {
                if(strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0){
                    //ignore "." and ".."
                    inn.open(entry->d_name);
                    inn >> str;
                    if(str.find(subString) != string.npos)
                    {
                        // TODO check if this is the right place to use new
                        fileName = new FileName(str);
                        fileVal = new FileValue();
                        Emit2(fileName, fileVal);
                        //TODO check where to delete
                        // pointers are sent to emit function which puts it in
                        // data structure (container) according to current
                        // thread.
                        // need to make sure we free all allocated memory
                        // correctly.
                    }
                }
                closedir(pDIR);
            }
        }
    }

    void Reduce(const k2Base *const key, const V2_LIST &vals) const
    {
        FileCount* fileCount;
        FileName* fileName = const_cast<FileName*>(&key);
        FileValue* fileVal;
        int counter = 0;
        for(v2Base* val : vals)
        {
            fileVal = const_cast<FileValue*>(&val);
            counter += fileVal->getFileVal();
        }
        fileCount = new FileCount(counter);
        Emit3(fileName, fileCount);
        //TODO check where to delete
        //delete(fileCount);
        //delete(fileName);
    }
};

int main(int argc, char* argv[])
{
    if (argc == 0) {
        std::cerr << "Usage: <substring to search> <folders, separated by space>" << endl;
        return 1;
    }

    //get the substring need to check and store it in a global variable so map
    // and reduce can use it
    subString = argv[0];

    IN_ITEMS_LIST directories;
    OUT_ITEMS_LIST result;
    DirNameKey* key;
    DirVal* val;
    IN_ITEM* dirPair;


    for(int i = 1; i < argc; i++)
    {
        key = new DirNameKey(argv[i]);
        val = new DirVal();
        dirPair = new pair(key, val);
        directories.push_back(*dirPair);
    }

    SubStringMapReduce* reducer = new SubStringMapReduce();
    result = runMapReduceFramework(*reducer, directories, multiThreadLevel);

    FileName* file;
    FileCount* fileCount;
    for(OUT_ITEM item : result)
    {
        fileCount = const_cast<FileCount*>(item.second);
        int count = fileCount->getFileCount();
        for(int i = 0;i < count;++i)
        {
            file = const_cast<FileName*>(item.first);
            cout << file->getFileName() << endl;
        }
    }

    //TODO is it enough to use clear? should we iterate and actually delete each pointer?
    directories.clear();
    result.clear();

}