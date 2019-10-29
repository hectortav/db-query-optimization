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
  int rowsz;
  int rows;
  int tmpcntr;
  list(int lnodesz,int rowsz);
  ~list();
  bool insert(uint64_t num);
  void print();
  uint64_t** lsttoarr();
};



#endif
