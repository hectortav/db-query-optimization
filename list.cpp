#include "list.h"


list::list(int lnodesz,int rowsz)
{
    this->first=NULL;
    this->last=NULL;
    this->lnodesz=lnodesz;
    this->rowsz=rowsz;
    this->rows=0;
    this->tmpcntr=0;
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
bool list::insert(uint64_t num)
{
    if(this->last==NULL)
        this->first=this->last=new listnode(this->lnodesz);
    if(sizeof(num)+this->last->size>this->lnodesz)
        this->last=this->last->next=new listnode(this->lnodesz);
    memcpy(this->last->content+this->last->size,&num,sizeof(num));
    tmpcntr++;
    if(tmpcntr==rowsz)
    {
        tmpcntr=0;
        rows++;
    }
    this->last->size+=sizeof(num);
    return true;
}

void list::print()
{
    if(this->first==NULL)
    {
        std::cout<<"No joined pairs"<<std::endl;
        return;
    }
    listnode* t=this->first;
    int cntr=0;
    while(t!=NULL)
    {
        uint64_t n;
        for(int i=0;i<t->size;i+=sizeof(uint64_t))
        {
            memcpy(&n,t->content+i,sizeof(uint64_t));
            std::cout<<n<<" ";
            cntr++;
            if(cntr==this->rowsz)
            {
                std::cout<<std::endl;
                cntr=0;
            }
        }
        t=t->next;
    }
}
uint64_t** list::lsttoarr()
{
    if(this->first==NULL)
        return NULL;
    
    uint64_t** arr;
    arr=new uint64_t*[rowsz];
    for(int i=0;i<rowsz;i++)
        arr[i]=new uint64_t[rows];
    
    listnode* t=this->first;
    int cntr=0;
    int row=0;
    while(t!=NULL)
    {
        uint64_t n;
        for(int i=0;i<t->size;i+=sizeof(uint64_t))
        {
            memcpy(&n,t->content+i,sizeof(uint64_t));
            arr[cntr][row]=n;
            cntr++;
        
            if(cntr==this->rowsz)
            {
                cntr=0;
                row++;
            }
        }
        t=t->next;
    }
    return arr;
}
bool list::insert(char ch)
{
    if(this->last==NULL)
    {
        this->first=this->last=new listnode(this->lnodesz);
        this->tmpcntr++;
    }
    if(sizeof(ch)+this->last->size>this->lnodesz)
    {
        this->last=this->last->next=new listnode(this->lnodesz);
        this->tmpcntr++;
    }
    memcpy(this->last->content+this->last->size,&ch,sizeof(ch));
    this->last->size+=sizeof(ch);
    return true;
}
char* list::lsttocharr()
{
    if(this->first==NULL)
        return NULL;
    
    char* arr;
    arr=new char[tmpcntr*lnodesz +1];
    listnode* t=this->first;
    for(int i=0;i<tmpcntr;i++,t=t->next)
    {
        memcpy(arr+i*lnodesz,t->content,lnodesz);
    }
    return arr;
}

listnode::listnode(int sz)
{
    this->next=NULL;
    this->content=new char[sz];
    this->size=0;
}
listnode::~listnode()
{
    delete[] this->content;
}