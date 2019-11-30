#include "functions.h"

//using namespace std;
//tuple is in std :/

int main(void)
{
    /*srand(time(NULL));uint64_t** r, **s;
    int rxnum,rynum,sxnum,synum;
    rxnum=4;rynum=3;
    sxnum=3;synum=5;
    r=new uint64_t*[rxnum];
    for(int i=0;i<rxnum;i++)
    {
        r[i]=new uint64_t[rynum];
        for(int j=0;j<rynum;j++)
        {   
            r[i][j]=rand()%10;
        }
        
        
    }
    s=new uint64_t*[sxnum];
    for(int i=0;i<sxnum;i++)
    {
        s[i]=new uint64_t[synum];
        for(int j=0;j<synum;j++)
        {   
            s[i][j]=rand()%10;
        }
    }
    for(int i=0;i<rynum;i++)
    {
        for(int j=0;j<rxnum;j++)
            std::cout<<r[j][i]<<" ";
        std::cout<<"\n";
    }
    std::cout<<"\n";
    for(int i=0;i<synum;i++)
    {
        for(int j=0;j<sxnum;j++)
            std::cout<<s[j][i]<<" ";
        std::cout<<"\n";
    }
    relation R,S;
    R.num_tuples=rynum;
    S.num_tuples=synum;
    int col;
    std::cout<<"\nWhich column? ";
    std::cin>>col;
    while(col<1||col>rxnum||col>sxnum)
    {
        std::cout<<"invalid index! Which column? ";
        std::cin>>col;
    }
    col--;
    extractcolumn(R,r,col);
    extractcolumn(S,s,col);
    R.print();
    std::cout<<"\n";
    S.print();
    relation* new_rel_R = new relation();
    new_rel_R->num_tuples=R.num_tuples;
    new_rel_R->tuples = new tuple[R.num_tuples];
    relation* ro_R=re_ordered(&R,new_rel_R,0);
    std::cout<<"\n";
    ro_R->print();
    relation* new_rel_S = new relation();
    new_rel_S->num_tuples=S.num_tuples;
    new_rel_S->tuples = new tuple[S.num_tuples];
    relation* ro_S=re_ordered(&S,new_rel_S,0);
    std::cout<<"\n";
    ro_S->print();
    std::cout<<"\n";

    result* rslt=join(ro_R,ro_S,r,s,rxnum,sxnum,col);
    rslt->lst->print();
    std::cout<<"\n";
    /*uint64_t** fnl=rslt->lst->lsttoarr();
    int fnlx=rslt->lst->rowsz;
    int fnly=rslt->lst->rows;
    if(fnl!=NULL)
    {
        for(int i=0;i<fnly;i++)
        {
            for(int j=0;j<fnlx;j++)
            {
                std::cout<<fnl[j][i]<<" ";
            }
            std::cout<<"\n";
        }
        for(int i=0;i<fnlx;i++)
        {
            delete[] fnl[i];
        }
        delete[] fnl;
    }*/

    /*uint64_t** fnl=rslt->lst->lsttoarr();
    for(int i=0;i<rslt->lst->rows;i++)
    {
        for(int j=0;j<rslt->lst->rowsz;j++)
        {
            std::cout<<fnl[j][i]<<" ";
        }
        std::cout<<std::endl;
    }


    delete new_rel_R;
    delete new_rel_S;
    for(int i=0;i<rxnum;i++)
    {
        delete[] r[i];
    }
    delete[] r;
    for(int i=0;i<sxnum;i++)
    {
        delete[] s[i];
    }
    delete[] s;
    delete rslt->lst;
    delete rslt;
    // R.num_tuples = 10;
    // R.tuples = new tuple[R.num_tuples]/* {{0xF}, {0x1}, {0x1}, {0xF}, {0x3}, {0x3}, {0xA}, {0xA}, {0xC}, {0xC}};*/
    // {
    //     {5, 0xBFF}, {6, 0xAFF},
    //     {0, 0xBAA}, {1, 0xBAA}, {2, 0xAAA}, {3, 0xCAA}, {4, 0xCAA},
    //     {7, 0xCCC}, {8, 0xBCC}, {9, 0xACC}
    //                                     };
    // std::cout << "before" << std::endl;
    // R.print();
    // relation *ro_R = re_ordered(&R, 0);
    // // quickSort(R.tuples, 0, R.num_tuples - 1);
    // std::cout << "\nafter" << std::endl;
    // ro_R->print();

    // delete ro_R;

    InputArray** inputArrays = readArrays();
    for (int i = 0; i < MAX_INPUT_ARRAYS_NUM; i++) {
        if (inputArrays[i] != NULL) {
            inputArrays[i]->print();
        }
    }

    int lines;
    char** arr=readbatch(lines);
    std::cout<<std::endl;
    for(int i=0;i<lines;i++)
    {
        std::cout<<arr[i]<<std::endl;
        handlequery(makeparts(arr[i]), inputArrays);
    }

}
