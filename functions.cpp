#include "functions.h"

void relation::print()
{
    for(int i=0;i<this->num_tuples;i++)
    {
        std::cout<<this->tuples[i].key<<". "<<this->tuples[i].payload<<std::endl;
    }
}

result* join(relation* R, relation* S)
{
    /*std::cout<<"join"<<std::endl;
    int64_t** array;
    array=new int64_t*[50000];
    for(int i=0;i<50000;i++)
        array[i]=new int64_t[2]{-1,-1};
    int samestart=-1;
    std::cout<<"aloced"<<std::endl;*/
    int samestart=-1;
    list* lst=new list(5);
    for(int r=0,s=0,i=0;r<R->num_tuples&&s<S->num_tuples;)
    {
        //std::cout<<"checking: R:"<<R->tuples[r].payload<<" S:"<<S->tuples[s].payload<<std::endl;
        int dec=R->tuples[r].payload-S->tuples[s].payload;
        if(dec==0)
        {
            //std::cout<<R->tuples[r].payload<<" same"<<std::endl; 
            char x[20];
            sprintf(x,"%ld %ld\n",R->tuples[r].key,S->tuples[s].key);
            lst->insert(x);
            std::cout<<i<<". "<<R->tuples[r].key<<" "<<S->tuples[s].key<<std::endl;
            //i++;
            //std::cout<<"Matching: R:"<<R->tuples[r].key<<" S:"<<S->tuples[s].key<<std::endl;
            /*array[i][0]=R->tuples[r].key;
            array[i][1]=R->tuples[s].key;
            i++;*/
            switch(samestart)
            {
                case -1:
                    if(s+1<S->num_tuples&&S->tuples[s].payload==S->tuples[s+1].payload)
                        samestart=s;
                default:
                    if(S->tuples[samestart].payload!=S->tuples[s].payload)
                        samestart=-1;
            }
            if(s+1<S->num_tuples&&S->tuples[s].payload==S->tuples[s+1].payload)
            {
                s++;
            }
            else
            {
                r++;
                if(samestart>-1)
                {
                    s=samestart;
                    samestart=-1;
                }
            }
            continue;
        }
        else if(dec<0)
        {
            //std::cout<<R->tuples[r].payload<<" <(r)"<<std::endl;
            r++;
            /*if(samestart!=-1)
            {
                s=samestart;
                samestart=-1;
            }*/
            continue;
        }
        else
        {
            //std::cout<<R->tuples[r].payload<<" >(s)"<<std::endl;
            s++;
            continue;
        }
    }
    /*std::cout<<"got out"<<std::endl;
    for(int i=0;i<50000;i++)
    {
        if(array[i][0]==-1&&array[i][1]==-1)
            break;
        std::cout<<i<<". "<<array[i][0]<<" "<<array[i][1]<<std::endl;
    }*/
    std::cout<<std::endl;
    lst->print();
}
#include <math.h>

void create_hist(relation *relR)
{
    int hist[pow(2, 8)][2] //static array?
    int64_t key;
    int i;
    for(i = 0; i < pow(2, 8); i++)
    {
        hist[i][0]= i;
        hist[i][1]= 0;
    }

    for (i = 0; i < relation.num_tuples; i++)
    {
        key = (relR.tuples[i].key >> 56) & 0xFF;
        hist[key][1]++;
    }
    

}
