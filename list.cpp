#include "list.h"


list::list(int lnodesz)
{
    this->first=NULL;
    this->last=NULL;
    this->lnodesz=lnodesz;
}
list::~list()
{
    listnode* t=this->first;
    while(this->first!=NULL)
    {
        this->first=t->next;
        delete t;
        t=this->first;
    }
}
bool list::insert(char* txt)
{
    if(this->last==NULL)
        this->first=this->last=new listnode(this->lnodesz);
    int ssz=strlen(txt);
    if(ssz+this->last->size>this->lnodesz)
        this->last=this->last->next=new listnode(this->lnodesz);
    strcat(this->last->content,txt);
}
void list::print()
{
    listnode* t=this->first;
    while(t!=NULL)
    {
        std::cout<<t->content;
        t=t->next;
    }
}


listnode::listnode(int sz)
{
    this->next=NULL;
    this->content=new char[sz];
    this->content[0]='\0';
    this->size=0;
}
listnode::~listnode()
{
    delete[] this->content;
}