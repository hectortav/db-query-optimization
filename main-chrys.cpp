#include "functions.h"


int main(void)
{
    srand(time(NULL));

    int64_t** r, **s;
    int rxnum,rynum,sxnum,synum;
    rxnum=4;rynum=10;
    sxnum=3;synum=5;
    r=new int64_t*[rxnum];
    for(int i=0;i<rxnum;i++)
    {
        r[i]=new int64_t[rynum];
        if(i==0)
        {
            for(int j=0;j<rynum;j++)
            {   
                r[i][j]=-1;
            }
        }
        else
        {
            for(int j=0;j<rynum;j++)
            {   
                r[i][j]=rand()%10;
            }
        }
        
    }
    s=new int64_t*[sxnum];
    for(int i=0;i<sxnum;i++)
    {
        s[i]=new int64_t[synum];
        if(i==0)
        {
            for(int j=0;j<synum;j++)
            {   
                s[i][j]=-1;
            }
        }
        else
        {
            for(int j=0;j<synum;j++)
            {   
                s[i][j]=rand()%10;
            }
        }
    }
    /*for(int i=0;i<rynum;i++)
    {
        for(int j=0;j<rxnum;j++)
        {
            std::cout<<r[j][i]<<" ";
        }
        std::cout<<std::endl;
    }
    std::cout<<std::endl;
    for(int i=0;i<synum;i++)
    {
        for(int j=0;j<sxnum;j++)
        {
            std::cout<<s[j][i]<<" ";
        }
        std::cout<<std::endl;
    }
    std::cout<<std::endl;*/

    relation R;
    relation S;
    int rsz=10;
    int ssz=5;
    R.tuples=new tuple[rsz];
    if(R.tuples==NULL)
        return -1;
    R.num_tuples=rsz;    
    S.tuples=new tuple[ssz];
    if(S.tuples==NULL)
        return -1;
    S.num_tuples=ssz;
    for(int i=0,w=0;i<R.num_tuples;w++)
    {
        int rnd=rand()% 2;
        if(rnd)
        {
            int place;
            while(1)
            {
                place=rand()%rynum;
                if(r[0][place]==-1)
                {
                    r[0][place]=w;
                    break;
                }
            }
            R.tuples[i].payload=w;
            R.tuples[i].key=place;
            i++;
        }
        rand()%2?--w:w=w;
        //std::cout<<i<<" "<<w<<" "<<r<<std::endl;
    }
    for(int i=0,w=0;i<S.num_tuples;w++)
    {
        int rnd=rand()% 2;
        if(rnd)
        {
            int place;
            while(1)
            {
                place=rand()%synum;
                if(s[0][place]==-1)
                {
                    s[0][place]=w;
                    break;
                }
            }
            S.tuples[i].payload=w;
            S.tuples[i].key=place;
            i++;
        }
        rand()%2?--w:w=w;
        //std::cout<<i<<" "<<w<<" "<<r<<std::endl;
    }
    /*for(int i=0;i<rynum;i++)
    {
        for(int j=0;j<rxnum;j++)
        {
            std::cout<<r[j][i]<<" ";
        }
        std::cout<<std::endl;
    }
    std::cout<<std::endl;
    for(int i=0;i<synum;i++)
    {
        for(int j=0;j<sxnum;j++)
        {
            std::cout<<s[j][i]<<" ";
        }
        std::cout<<std::endl;
    }
    std::cout<<std::endl;
    /*R.tuples[0].payload=1;R.tuples[0].key=0;
    R.tuples[1].payload=1;R.tuples[1].key=1;
    R.tuples[2].payload=2;R.tuples[2].key=2;
    R.tuples[3].payload=2;R.tuples[3].key=3;
    R.tuples[4].payload=2;R.tuples[4].key=4;
    R.tuples[5].payload=6;R.tuples[5].key=5;
    R.tuples[6].payload=7;R.tuples[6].key=6;
    R.tuples[7].payload=8;R.tuples[7].key=7;
    R.tuples[8].payload=9;R.tuples[8].key=8;
    R.tuples[9].payload=10;R.tuples[9].key=9;

    S.tuples[0].payload=1;S.tuples[0].key=0;
    S.tuples[1].payload=1;S.tuples[1].key=1;
    S.tuples[2].payload=2;S.tuples[2].key=2;
    S.tuples[3].payload=3;S.tuples[3].key=3;
    S.tuples[4].payload=5;S.tuples[4].key=4;*/

    R.print();
    std::cout<<std::endl;
    S.print();
    std::cout<<std::endl;

    for(int i=0;i<R.num_tuples;i++)
    {
        std::cout<<R.tuples[i].key<<". "<<R.tuples[i].payload<<" ";
        for(int j=1;j<rxnum;j++)
        {
            std::cout<<r[j][R.tuples[i].key]<<" ";
        }
        std::cout<<std::endl;
    }
    std::cout<<std::endl;
    for(int i=0;i<S.num_tuples;i++)
    {
        std::cout<<S.tuples[i].key<<". "<<S.tuples[i].payload<<" ";
        for(int j=1;j<sxnum;j++)
        {
            std::cout<<s[j][S.tuples[i].key]<<" ";
        }
        std::cout<<std::endl;
    }

    //R.print();
    //std::cout<<std::endl;
    //S.print();
    std::cout<<std::endl;
    result* rslt=join(&R,&S,r,s,rxnum,sxnum,0);
    rslt->lst->print();
    
}
