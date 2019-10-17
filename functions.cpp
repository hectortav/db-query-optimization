#include "functions.h"

void relation::print()
{
    for(int i=0;i<this->num_tuples;i++)
    {
        std::cout<<this->tuples[i].key<<". "<<this->tuples[i].payload<<std::endl;
    }
}

relation::relation()
{
    num_tuples = 0;
    tuples = NULL;
}

relation::~relation()
{
    if (tuples != NULL)
        delete [] tuples;
}

result* join(relation* R, relation* S)
{
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
            //std::cout<<i<<". "<<R->tuples[r].key<<" "<<S->tuples[s].key<<std::endl;
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

int64_t** create_hist(relation *rel)
{
    int x = pow(2,8);
    int64_t **hist = new int64_t*[2];
    for(int i = 0; i < 2; i++)
        hist[i] = new int64_t[x];
    int64_t key;
    for(int i = 0; i < x; i++)
    {
        hist[0][i]= i;
        hist[1][i]= 0;
    }

    for (int i = 0; i < rel->num_tuples; i++)
    {
        //key = (rel->tuples[i].key >> 56) & 0xFF;
        key = rel->tuples[i].key & 0xFF;
        hist[1][key]++;
    }
    return hist;
}

int64_t** create_psum(int64_t** hist)
{
    int count = 0;
    int x = pow(2,8);
    int64_t **psum = new int64_t*[2];
    for(int i = 0; i < 2; i++)
        psum[i] = new int64_t[x];

    for (int i = 0; i < x; i++)
    {
        psum[0][i] = hist[0][i];
        psum[1][i] = (int64_t)count;
        count+=hist[1][i];
    }
    return psum;
}

relation* re_ordered(relation *rel)
{
    relation *new_rel = new relation();
    //create histogram
    int64_t** hist = create_hist(rel);
    int64_t** psum = create_psum(hist);
    int x = pow(2, 8);
    int64_t key;

    new_rel->num_tuples = rel->num_tuples;
    new_rel->tuples = new tuple[new_rel->num_tuples];
    bool *flag = new bool[new_rel->num_tuples];
    for (int i = 0; i < rel->num_tuples; i++)
    {
        flag[i] = false;
    }

    //testing
    /*for (int i = 0; i < x; i++)
        if (i == x-1 || psum[1][i] != psum[1][i+1])
            std::cout << psum[0][i] << " " << psum[1][i] << std::endl;*/

    for (int i = 0; i < rel->num_tuples; i++)
    {
        //hash
        key = rel->tuples[i].key & 0xFF;
        //find hash in psum = pos in new relation
        key = psum[1][key];
        //key++ until their is an empty place
        while (flag[key] == true && key < rel->num_tuples)
            key++;
        //std::cout << key << std::endl;
        if (key < rel->num_tuples)
        {
            new_rel->tuples[key].key = rel->tuples[i].key;
            new_rel->tuples[key].payload = rel->tuples[i].payload;
            flag[key] = true;
        }
    }   

    delete [] hist[0];
    delete [] hist[1];
    delete [] hist;
    
    delete [] psum[0];
    delete [] psum[1];
    delete [] psum;
    delete [] flag;


    return new_rel;

}