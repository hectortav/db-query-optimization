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
    //evala Return gia na mhn vgazei Warning
    return NULL;
}

int64_t** create_hist(relation *rel, int shift)
{
    int x = pow(2,8);
    int64_t **hist = new int64_t*[2];
    for(int i = 0; i < 2; i++)
        hist[i] = new int64_t[x];
    int64_t payload, mid;
    for(int i = 0; i < x; i++)
    {
        hist[0][i]= i;
        hist[1][i]= 0;
    }

    for (int i = 0; i < rel->num_tuples; i++)
    {
        mid = (0xFFFFFFFF & rel->tuples[i].payload) >> (8*shift);
        payload = mid & 0xFF;
        if (payload > 0xFF)
        {
            std::cout << "ERROR " << payload << std::endl;
            return NULL;
        }
        hist[1][payload]++;
    }
    return hist;
}

int64_t** create_psum(int64_t** hist)
{
    int count = 0;
    int x = pow(2,8);
    int64_t **psum = new int64_t*[3];
    for(int i = 0; i < 3; i++)
        psum[i] = new int64_t[x];

    for (int i = 0; i < x; i++)
    {
        psum[0][i] = hist[0][i];
        psum[1][i] = (int64_t)count;
        psum[2][i] = 0; //how many times we hashed
        count+=hist[1][i];
    }
    return psum;
}

relation* re_ordered(relation *rel, int shift)
{
    relation *new_rel = new relation();
    relation *temp;
    relation *rtn;
    //create histogram
    int64_t** hist = create_hist(rel, shift);
    //create psum
    int64_t** psum = create_psum(hist);
    int x = pow(2, 8);
    int64_t payload;
    int i, j, y;

    new_rel->num_tuples = rel->num_tuples;
    new_rel->tuples = new tuple[new_rel->num_tuples];
    bool *flag = new bool[new_rel->num_tuples];
    for (i = 0; i < new_rel->num_tuples; i++)
        flag[i] = false;

    i = 0;
    while(i < rel->num_tuples)
    {
        //hash
        payload = (0xFFFFFFFF & rel->tuples[i].payload) >> (8*shift) & 0xFF;
        //find hash in psum = pos in new relation
        payload = psum[1][payload];

        //payload++ until their is an empty place
        while ((payload < rel->num_tuples) && flag[payload])
            payload++;

        if (payload < rel->num_tuples)
        {
            new_rel->tuples[payload].payload = rel->tuples[i].payload;
            new_rel->tuples[payload].key = rel->tuples[i].key;
            flag[payload] = true;
        }
        i++;
    }

    //testing
    /*for (int i = 0; i < x; i++)
        if (i == x-1 || psum[1][i] != psum[1][i+1])
            std::cout << psum[0][i] << " " << psum[1][i] << std::endl;
    std::cout << "<<<<<<<" << std::endl;*/

    i = 0;
    while (i < x)
    {
        if ((hist[1][i] > TUPLES_PER_BUCKET) && ((shift + 1) * 8 <= 64))
        {
            // new rel to re_order
            temp = new relation();
            temp->num_tuples = hist[1][i];
            temp->tuples = new tuple[temp->num_tuples];
            j = psum[1][i];
            y = 0;
            while (j < x)
            {   
                if (i + 1 < x)
                    if (j >= psum[1][i+1])
                        break;
                temp->tuples[y].payload = new_rel->tuples[j].payload;
                temp->tuples[y].key = new_rel->tuples[j].key;
                j++;
                y++;
            }
            rtn = re_ordered(temp, shift+1);
            j = psum[1][i];
            y = 0;
            while (j < x)
            {
                if (j >= psum[1][i+1])
                    break;
                new_rel->tuples[j].payload = rtn->tuples[y].payload;
                new_rel->tuples[j].key = rtn->tuples[y].key;
                j++;
                y++;
            }
            delete rtn;
            delete temp;
        }
        i++;
    }

    // std::cout << "before sort" << std::endl;
    // new_rel->print();
    i = 0;
    while (i < x)
    {
        if (hist[1][i] > 0)
        {
            if (i + 1 < x)
                sortBucket(new_rel, psum[1][i], psum[1][i+1]);
            else
                sortBucket(new_rel, psum[1][i], new_rel->num_tuples);
        }
        i++;
    }

    // sortBucket(new_rel, 0, 4);

    // std::cout << "after sort" << std::endl;
    // new_rel->print();

    delete [] hist[0];
    delete [] hist[1];

    delete [] hist;
    
    delete [] psum[0];
    delete [] psum[1];
    delete [] psum[2];

    delete [] psum;
    delete [] flag;

    return new_rel;

}

void swap(tuple* tuple1, tuple* tuple2)
{
    int64_t tempKey = tuple1->key;
    int64_t tempPayload = tuple1->payload;

    tuple1->key = tuple2->key;
    tuple1->payload = tuple2->payload;

    tuple2->key = tempKey;
    tuple2->payload = tempPayload;
}

int randomIndex(int startIndex, int stopIndex) {
    srand(time(NULL));

    return rand()%(stopIndex - startIndex + 1) + startIndex;
}

int partition(tuple* tuples, int startIndex, int stopIndex)
{ 
    int pivotIndex = randomIndex(startIndex, stopIndex);

    int64_t pivot = tuples[pivotIndex].payload;

    swap(&tuples[pivotIndex], &tuples[stopIndex]);

    int i = startIndex - 1;  // index of smaller element 
  
    for (int j = startIndex; j < stopIndex; j++) 
    {
        if (tuples[j].payload < pivot) 
        { 
            // if current element is smaller than the pivot 
            i++;    // increment index of smaller element 
            
            swap(&tuples[i], &tuples[j]);
        }
    }
    swap(&tuples[i + 1], &tuples[stopIndex]);
    return (i + 1);
}


void quickSort(tuple* tuples, int startIndex, int stopIndex)
{
    if (startIndex < stopIndex) 
    { 
        int partitionIndex = partition(tuples, startIndex, stopIndex); 
  
        quickSort(tuples, startIndex, partitionIndex - 1); 
        quickSort(tuples, partitionIndex + 1, stopIndex); 
    } 
}

// (startIndex, stopIndex) -> inclusive
void sortBucket(relation* rel, int startIndex, int stopIndex) {
    // stopIndex--;
    quickSort(rel->tuples, startIndex, stopIndex);
}