#ifndef _LIST_H
#define _LIST_H
#include "functions.h"


class listnode{
public:
  char* content;
  int size;
  listnode* next;
  listnode(int sz);
  ~listnode();
};

class list{
public:
  listnode* first;
  listnode* last;
  int lnodesz;
  list(int lnodesz);
  ~list();
  bool insert(char* txt);
  void print();
};



#endif
