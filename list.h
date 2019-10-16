#ifndef _LIST_H
#define _LIST_H
#include "functions.h"


class listnode{
  void* content;
  listnode* next;
  void insert();
  listnode();
  ~listnode();
};

#endif
