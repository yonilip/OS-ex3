

#include "MapReduceFrameWork.h"
#include <iostream>
#include <fstream>
#include <string.h>
#include <sstream>
#include <dirent.h>

#define THREAD_LEVEL 7

int multiThreadLevel = THREAD_LEVEL;

/** we get the substring as an argument to main, but map and reduce need
  this variable to work properly, the best solution i found was to make it
 global.. need to check it is legit
 **/
std::string subString;


/**
 * inherits from k1 Base, holds directory name
 */
class DirNameKey : public k1Base
{
private:
    //std::string dirName;
	char* dirName;
public:
    //DirNameKey(std::string& dirName) : dirName(dirName) {};
	DirNameKey(char* dirName) : dirName(dirName) {};


	char* getDirName()
    {
        return dirName;
    }

    bool operator<(const k1Base &other) const
    {
		return dirName < ((DirNameKey*)&other)->dirName;
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
    std::string fileName;
public:
    FileName(std::string& fileName) : fileName(fileName){};
    std::string getFileName()
    {
        return fileName;
    }
    bool operator<(const k2Base &other) const
    {
       /* FileName* otherStr = const_cast<FileName*>(&other);
        return (fileName < otherStr->fileName);*/ //TODO del
		return fileName < ((FileName*)&other)->fileName;
    }

	bool operator<(const k3Base &other) const
	{
		return fileName < ((FileName*)&other)->fileName;
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
		DirNameKey* dir = ((DirNameKey*)&key);
        //std::string dirName = dir->getDirName();
		char* dirName = dir->getDirName();
        //const char* cStr = dirName.c_str(); //makes string into char*
        std::ifstream inn;
        std::string str;

        /**
         * the idea is to parse given directory given as key and add all file
         * in it to list using Emit function that was given to us
         */
		DIR* pDIR = opendir(dirName);
        if(pDIR)
        {
			struct dirent* entry = readdir(pDIR);
            while(entry)
            {
				//ignore "." and ".."
                if(strcmp(entry->d_name, ".") != 0 &&
						strcmp(entry->d_name, "..") != 0){
                    inn.open(entry->d_name);
                    inn >> str;
					if (str.find(subString) != str.npos)
                    {
						FileName* fileName = new FileName(str); //TODO maybe make global container that holds these pointers for deletion
						FileValue* fileVal = new FileValue();
                        Emit2(fileName, fileVal);
                        //TODO check where to delete
                    }
                }
                closedir(pDIR);
            }
        }
    }

    void Reduce(const k2Base *const key, const V2_LIST &vals) const
    {
        FileCount* fileCount;
        //FileName* fileName = const_cast<FileName*>(&key);
		FileName* fileName = ((FileName*)&key);
        FileValue* fileVal;
        int counter = 0;
        for(v2Base* val : vals)
        {
            //fileVal = const_cast<FileValue*>(&val);
			fileVal = ((FileValue*)&val);
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
    if (argc < 2) {
        std::cerr << "Usage: <substring to search> "
							 "<folders, separated by space>" << std::endl;
        return 1;
    }

    //get the substring need to check and store it in a global variable so map
    // and reduce can use it
    subString = argv[1];

    IN_ITEMS_LIST directories;
    OUT_ITEMS_LIST result;

    for(int i = 2; i < argc; i++)
    {
		//std::string str = argv[i];
        k1Base* key = new DirNameKey(argv[i]);
		//std::string test = ((DirNameKey*)&key)->getDirName(); //TODO del
        v1Base* val = new DirVal();
        //dirPair = new std::pair(key, val); //TODO make pair and not pointers
		IN_ITEM dirPair = std::make_pair(key, val);
        directories.push_back(dirPair); //TODO does deleting the contents of this delete key and val?
    }

	SubStringMapReduce searchMapReduce;
    result = runMapReduceFramework(searchMapReduce, directories,
								   multiThreadLevel);

    for(OUT_ITEM item : result)
    {
		FileCount* fileCount = ((FileCount*)&item.second);
        int count = fileCount->getFileCount();
        for(int i = 0;i < count; ++i)
        {
			FileName* file = ((FileName*)&item.first);
            std::cout << file->getFileName() << std::endl;
        }
    }

    //TODO is it enough to use clear? should we iterate and actually delete each pointer?
    directories.clear();
    result.clear();

}