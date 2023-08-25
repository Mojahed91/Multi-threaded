#include <string>
#include <iostream>
#include <fstream>
#include <string.h>
#include <sstream>
#include <dirent.h>
#include <stdlib.h>

#define THREAD_LEVEL 7

// Global variable to define the number of threads to be used in parallel.
int multiThreadLevel = THREAD_LEVEL;

// Substring to search for in the directory files.
std::string subString;

// Checks the return value of system calls and prints an error if needed.
void searchCheckSysCall(int res)
{
    if (res != 0)
    {
        std::cout << "error" << std::endl;
        exit(1);
    }
}

// Represents the directory name key for the Map function.
class k1 : public k1Base
{
public:
    std::string dirName;
    k1(std::string dirName) : dirName(dirName) {};
    ~k1(){}

    // Overloaded < operator for sorting/comparing.
    bool operator<(const k1Base &other) const
    {
        return dirName < ((const k1&)other).dirName;
    }
};

// Placeholder value for a directory.
class v1 : public v1Base
{
public:
    int dirVal;
    v1() : dirVal(0){}
    ~v1(){}
};

// Represents the filename key for the Map and Reduce functions.
class k2 : public k2Base
{
public:
    std::string fileName;
    k2(std::string fileName) : fileName(fileName){};
    ~k2(){}

    bool operator<(const k2Base &other) const
    {
        return fileName < ((const k2&)other).fileName;
    }
};

// Represents the value associated with a filename for the Map function.
class v2 : public v2Base
{
public:
    int fileVal;
    
    v2() : fileVal(1){};
    ~v2(){}
};

// Represents the filename key for the output of the Reduce function.
class k3 : public k3Base
{
public:
    std::string fileName;
    k3(std::string fileName) : fileName(fileName){};
    ~k3(){}

    bool operator<(const k3Base &other) const
    {
        return fileName < ((const k3&)other).fileName;
    }
};

// Represents the number of times a filename appears for the Reduce function.
class v3 : public v3Base
{
public:
    int fileCount;
    
    v3(int count) : fileCount(count){};
    ~v3(){}
};

// Vector to store the final results.
OUT_ITEMS_VEC result;

// Vector to store directory input items.
IN_ITEMS_VEC directories;

// The main MapReduce class implementation for the SubString search.
class SubStringMapReduce : public MapReduceBase
{
    // Maps directory names to filenames if they contain the substring.
    void Map(const k1Base *const key, const v1Base *const val) const
    {
        k1* dir = ((k1 *)key);
        std::string dirName = dir->dirName;
        (void)val;  // Unused

        DIR* pDIR = opendir(dirName.c_str());
        if(pDIR)
        {
            struct dirent* entry;
            entry = readdir(pDIR);
        
            while(entry)
            {
                if((strcmp(entry->d_name, ".") != 0) && (strcmp(entry->d_name, "..")) != 0 && (strstr(entry->d_name, subString.c_str())))
                {
                    k2* fileName = new k2((std::string)entry->d_name);
                    v2* fileVal = new v2();
                    Emit2((k2Base*)fileName, (v2Base*)fileVal);
                }
                entry = readdir(pDIR);
            }
            closedir(pDIR);
        }
    }

    // Reduces file names by counting occurrences.
    void Reduce(const k2Base *const key, const V2_VEC &vals) const
    {
        v3* fileCount;
        k3* fileName = ((k3*)key);
        v2* fileVal;
        int counter = 0;
        for(v2Base* val : vals)
        {
            fileVal = ((v2*)val);
            counter += fileVal->fileVal;
        }
        fileCount = new v3(counter);
        Emit3((k3Base* )fileName, (v3Base*)fileCount);
    }
};

// Prints the final result, showing each file that matched the substring.
void printRsult()
{
    for(OUT_ITEM item : result)
    {
        v3* fileCount = ((v3*)item.second);
        int count = fileCount->fileCount;
        for(int i = 0; i < count; ++i)
        {
            if(item == result[result.size()-1] && i == count-1)
            {
                break;
            }
            k3* file = ((k3*)item.first);
            std::cout << file->fileName << " ";
        }
    }
    
    k3* file = ((k3*)result[result.size()-1].first);
    std::cout << file->fileName;
}

// Cleans up dynamically allocated memory.
void destroy()
{
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
}

// Fills the directories vector with the provided directory arguments.
void goOverDir(int argc, char* argv[])
{
    for(int i = 2; i < argc; i++)
    {
        std::string str = argv[i];
        k1* key = new k1(str);
        v1* val = new v1();
        directories.push_back(std::make_pair((k1Base*) key, (v1Base*) val));
    }
}

// Main function of the program.
int main(int argc, char* argv[])
{
    if (argc < 2) 
    {
        std::cerr << "Usage: <substring to search> <folders, separated by space>" << std::endl;
        return 1;
    }

    subString = argv[1];
    goOverDir(argc, argv);

    SubStringMapReduce searchMapReduce;
    result = RunMapReduceFramework(searchMapReduce, directories, multiThreadLevel, true);
    printRsult();
    destroy();
}
