#include "besttreemap.h"

Statistics::Statistics(int min,int max,int numdiscrete,int size)
{
    this->min=min;
    this->max=max;
    this->numdiscrete=numdiscrete;
    this->size=size;
}
Statistics::~Statistics()
{

}

Array::Array()
{

}
Array::~Array()
{

}

Key::Key(int* arr,int sz)
{
    this->KeyArray=new Array();
    this->KeyArray->array=arr;
    this->KeyArray->size=sz;
}

Key::~Key()
{
    delete[] this->KeyArray;
}

Value::Value(int* arr, int sz)
{
    this->ValueArray=new Array();
    this->ValueArray->array=arr;
    this->ValueArray->size=sz;
    this->stats=NULL;
}
Value::~Value()
{
    delete this->ValueArray;
    if(stats!=NULL)
        delete stats;
}

int factorial(int x)
{
    int sum=0;
    for(int i=x;i>0;i--)
    {
        sum+=x;
    }
    return sum;
}

Map::Map(int queryArraysNum)
{
    values=new Value*[factorial(queryArraysNum)];
    keys=new Key*[factorial(queryArraysNum)];
    cursize=0;
}

Map::~Map()
{
    delete[] values;
    delete[] keys;
}

bool Map::insert(int* key,int keysize,int* value,int valuesize)
{
    int ifExistsKey=exists(key,keysize);
    if(ifExistsKey>=0)
    {
        delete values[ifExistsKey];
        values[ifExistsKey]=new Value(value,valuesize);
    }
    else
    {
        values[cursize]=new Value(value,valuesize);
        keys[cursize]=new Key(key,keysize);
        cursize++;
    }
} 
Value* Map::retrieve(int* key,int size)
{
    int ifExistsKey=exists(key,size);
    if(ifExistsKey>=0)
    {
        return values[ifExistsKey];
    }
}
int Map::exists(int* key,int keysize)
{
    bool found;
    for(int i=0;i<cursize;i++)
    {
        found=1;
        if(keys[i]->KeyArray->size==keysize&&keysize>0)
        {
            for(int j=0;j<keys[i]->KeyArray->size;j++)
            {
                if(keys[i]->KeyArray->array[j]!=key[j])
                {
                    found=0;
                    break;
                }
            }
            if(found)
                return i;
        }
    }
    return -1;
}
void Map::print()
{
    for(int i=0;i<cursize;i++)
    {
        if(values[i]->ValueArray->size<=0)
        {
            std::cout<<"empty value"<<std::endl;
        }
        if(keys[i]->KeyArray->size<=0)
        {
            std::cout<<"empty key"<<std::endl;
        }
        std::cout<<keys[i]->KeyArray->array[0];
        for(int j=1;j<keys[i]->KeyArray->size;j++)
        {
            std::cout<<", "<<keys[i]->KeyArray->array[j];
        }
        std::cout<<"  -->  ";
        std::cout<<values[i]->ValueArray->array[0];
        for(int j=1;j<values[i]->ValueArray->size;j++)
        {
            std::cout<<", "<<values[i]->ValueArray->array[j];
        }
        std::cout<<std::endl;
    }
}