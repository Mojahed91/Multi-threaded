#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"
#include <utility>

typedef std::pair<k1Base*, v1Base*> IN_ITEM;
typedef std::pair<k3Base*, v3Base*> OUT_ITEM;

typedef std::vector<IN_ITEM> IN_ITEMS_VEC; 
typedef std::vector<OUT_ITEM> OUT_ITEMS_VEC;

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec, 
									int multiThreadLevel, bool autoDeleteV2K2);

void Emit2 (k2Base*, v2Base*);
void Emit3 (k3Base*, v3Base*);

#endif //MAPREDUCEFRAMEWORK_H
