#ifndef MAPREDUCECLIENT_H
#define MAPREDUCECLIENT_H

#include <vector>

//input key and value.
//the key, value for the map function and the MapReduceFramework
class k1Base {
public:
	virtual ~k1Base(){}
    virtual bool operator<(const k1Base &other) const = 0;
};

class v1Base {
public:
	virtual ~v1Base() {}
};

//intermediate key and value.
//the key, value for the Reduce function created by the Map function
class k2Base {
public:
	virtual ~k2Base(){}
    virtual bool operator<(const k2Base &other) const = 0;
};

class v2Base {
public:
	virtual ~v2Base(){}
};

//output key and value
//the key,value for the Reduce function created by the Map function
class k3Base {
public:
	virtual ~k3Base()  {}
    virtual bool operator<(const k3Base &other) const = 0;
};

class v3Base {
public:
	virtual ~v3Base() {}
};

typedef std::vector<v2Base *> V2_VEC;

class MapReduceBase {
public:
    virtual void Map(const k1Base *const key, const v1Base *const val) const = 0;
    virtual void Reduce(const k2Base *const key, const V2_VEC &vals) const = 0;
};


#endif //MAPREDUCECLIENT_H
