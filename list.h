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
  uint64_t rows;
  uint64_t tmpcntr;
  list(int lnodesz,uint64_t rowsz);
  ~list();
  bool insert(uint64_t num);
  bool insert(char ch);
  void print();
  uint64_t** lsttoarr();
  char* lsttocharr();
};



#endif
