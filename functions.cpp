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

InputArray::InputArray(uint64_t rowsNum, uint64_t columnsNum) {
    this->rowsNum = rowsNum;
    this->columnsNum = columnsNum;
    this->columns = new uint64_t*[columnsNum];
    for (int i = 0; i < columnsNum; i++) {
        this->columns[i] = new uint64_t[rowsNum];
    }
}

InputArray::~InputArray() {
    for (int i = 0; i < columnsNum; i++) {
        delete[] this->columns[i];
    }
    delete[] this->columns;
}

unsigned char hashFunction(uint64_t payload, int shift) {
    return (payload >> (8 * shift)) & 0xFF;
}

result* join(relation* R, relation* S,uint64_t**rr,uint64_t**ss,int rsz,int ssz,int joincol)
{
    int samestart=-1;
    int lstsize=1024*1024;
    list* lst=new list(lstsize,rsz+ssz-1);
    for(int r=0,s=0,i=0;r<R->num_tuples&&s<S->num_tuples;)
    {
        //std::cout<<"checking: R:"<<R->tuples[r].payload<<" S:"<<S->tuples[s].payload<<std::endl;
        int dec=R->tuples[r].payload-S->tuples[s].payload;
        if(dec==0)
        {
            //std::cout<<R->tuples[r].payload<<" same"<<std::endl; 
            //char x[lstsize+2];
            //x[0]='\0';
            for(int i=0;i<rsz;i++)
            {
                //sprintf(x,"%s%ld ",x,rr[i][R->tuples[r].key]);
                lst->insert(rr[i][R->tuples[r].key]);
            }
            for(int i=0;i<ssz;i++)
            {
                if(joincol==i)
                    continue;
                //sprintf(x,"%s%ld ",x,ss[i][S->tuples[s].key]);
                lst->insert(ss[i][S->tuples[s].key]);
            }
            //sprintf(x,"%s\n",x);
            //printf("%s",x);
            //sprintf(x,"%ld %ld\n",R->tuples[r].key,S->tuples[s].key);
        //lst->insert(x);
            //std::cout<<i<<". "<<R->tuples[r].key<<" "<<S->tuples[s].key<<std::endl;
            //std::cout<<x;
            i++;
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
    result* rslt=new result;
    rslt->lst=lst;
    return rslt;
    //lst->print();
    //lst->print();
    //evala Return gia na mhn vgazei Warning
    return NULL;
}

uint64_t** create_hist(relation *rel, int shift)
{
    int x = pow(2,8);
    uint64_t **hist = new uint64_t*[2];
    for(int i = 0; i < 2; i++)
        hist[i] = new uint64_t[x];
    uint64_t payload, mid;
    for(int i = 0; i < x; i++)
    {
        hist[0][i]= i;
        hist[1][i]= 0;
    }

    for (int i = 0; i < rel->num_tuples; i++)
    {
        //mid = (0xFFFFFFFF & rel->tuples[i].payload) >> (8*shift);
        payload = hashFunction(rel->tuples[i].payload, 7-shift);

        if (payload > 0xFF)
        {
            std::cout << "ERROR " << payload << std::endl;
            return NULL;
        }
        hist[1][payload]++;
    }
    return hist;
}

uint64_t** create_psum(uint64_t** hist)
{
    int count = 0;
    int x = pow(2,8);
    uint64_t **psum = new uint64_t*[2];
    for(int i = 0; i < 2; i++)
        psum[i] = new uint64_t[x];

    for (int i = 0; i < x; i++)
    {
        psum[0][i] = hist[0][i];
        psum[1][i] = (uint64_t)count;
        count+=hist[1][i];
    }
    return psum;
}

relation* re_ordered(relation *rel, relation* new_rel, int shift)
{
    // relation *new_rel = new relation();
    relation *temp;
    relation *rtn;
    //create histogram
    uint64_t** hist = create_hist(rel, shift);
    //create psum
    uint64_t** psum = create_psum(hist);
    int x = pow(2, 8);
    uint64_t payload;
    int i, j, y;

    bool *flag = new bool[rel->num_tuples];
    for (i = 0; i < rel->num_tuples; i++)
        flag[i] = false;

    i = 0;
    while(i < rel->num_tuples)
    {
        //hash
        //payload = (0xFFFFFFFF & rel->tuples[i].payload) >> (8*shift) & 0xFF;
        payload = hashFunction(rel->tuples[i].payload, 7 - shift);
        //find hash in psum = pos in new relation
        
        int next_i = psum[1][payload];

        //key++ until their is an empty place
        while ((next_i < rel->num_tuples) && flag[next_i])
            next_i++;

        if (next_i < rel->num_tuples)
        {
            new_rel->tuples[next_i].payload = rel->tuples[i].payload;
            new_rel->tuples[next_i].key = rel->tuples[i].key;
            flag[next_i] = true;
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
        if ((hist[1][i] > TUPLES_PER_BUCKET) && (shift  < 7))
        {
            // new rel to re_order
            temp = new relation();
            temp->num_tuples = hist[1][i];
            temp->tuples = (new_rel->tuples + psum[1][i]);
            rtn = re_ordered(temp, rel, shift + 1);
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
            std::free(temp); // free only relation's pointer because the tuples are not taking additional space
        }
        else if (hist[1][i] > 0)
        {
                //print bucket before sort
                /*std::cout << "{" << std::endl;
                if (i + 1 < x)
                {
                    for (int l = psum[1][i]; l < psum[1][i+1]; l++)
                        std::cout << new_rel->tuples[l].payload << ". " << new_rel->tuples[l].key << std::endl;
                }
                else
                {
                    for (int l = psum[1][i]; l < new_rel->num_tuples; l++)
                        std::cout << new_rel->tuples[l].payload << ". " << new_rel->tuples[l].key << std::endl;
                }
                std::cout << "}" << std::endl;
            std::cout << std::endl;*/
            if (i + 1 < x)
            {
                //std::cout << "-sort- " << psum[1][i] << " " << psum[1][i+1] << std::endl;
                sortBucket(new_rel, psum[1][i], psum[1][i+1] - 1);
            }
            else
            {
                //std::cout << "-sort- " << psum[1][i] << " " << new_rel->num_tuples << std::endl;
                sortBucket(new_rel, psum[1][i], rel->num_tuples - 1);
            }
        }
        
        //print buckets
        /*if (hist[1][i] > 0)
        {
            if (i + 1 < x)
            {
                for (int l = psum[1][i]; l < psum[1][i+1]; l++)
                    std::cout << new_rel->tuples[l].payload << ". " << new_rel->tuples[l].key << std::endl;
            }
            else
            {
                for (int l = psum[1][i]; l < new_rel->num_tuples; l++)
                    std::cout << new_rel->tuples[l].payload << ". " << new_rel->tuples[l].key << std::endl;
            }
            std::cout << std::endl;
        }*/
        
        i++;
    }

    // std::cout << "before sort" << std::endl;
    // new_rel->print();

    // sortBucket(new_rel, 0, 4);

    // std::cout << "after sort" << std::endl;
    // new_rel->print();

    delete [] hist[0];
    delete [] hist[1];

    delete [] hist;
    
    delete [] psum[0];
    delete [] psum[1];

    delete [] psum;
    delete [] flag;

    return new_rel;
}

void swap(tuple* tuple1, tuple* tuple2)
{
    uint64_t tempKey = tuple1->key;
    uint64_t tempPayload = tuple1->payload;

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

    uint64_t pivot = tuples[pivotIndex].payload;

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

// (startIndex, stopIndex) -> inclusive
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

void extractcolumn(relation& rel,uint64_t **array, int column)
{
    rel.tuples=new tuple[rel.num_tuples];
    for(int i=0;i<rel.num_tuples;i++)
    {
        rel.tuples[i].key=i;
        rel.tuples[i].payload=array[column][i];
    }
}

InputArray** readArrays() {
    InputArray** inputArrays = new InputArray*[MAX_INPUT_ARRAYS_NUM]; // size is fixed

    size_t fileNameSize = MAX_INPUT_FILE_NAME_SIZE;
    char fileName[fileNameSize];

    unsigned int inputArraysIndex = 0;
    while (fgets(fileName, fileNameSize, stdin) != NULL) {
        fileName[strlen(fileName) - 1] = '\0'; // remove newline character

        uint64_t rowsNum, columnsNum, cellValue;
        FILE *fileP;

        fileP = fopen(fileName, "rb");

        fread(&rowsNum, sizeof(uint64_t), 1, fileP);
        fread(&columnsNum, sizeof(uint64_t), 1, fileP);

        inputArrays[inputArraysIndex] = new InputArray(rowsNum, columnsNum);

        for (uint64_t i = 0; i < columnsNum; i++) {
            for (uint16_t j = 0; j < rowsNum; j++) {
                fread(&inputArrays[inputArraysIndex]->columns[i][j], sizeof(uint64_t), 1, fileP);
            }
        }

        fclose(fileP);

        inputArraysIndex++;
    }

    return inputArrays;
}
char** readbatch(int& lns)
{
    char ch;
    list* l=new list(1024,0);
    int flag=0;
    int lines=0;
    while(1)
    {
        ch=getchar();
        if(ch=='\n'&&flag)
            continue;
        l->insert(ch);
        if(ch=='F'&&flag)
            break;
        if(ch=='\n')
        {
            flag=1;
            lines++;
        }
        else flag=0;
    }
    char* arr=l->lsttocharr();
    char** fnl=new char*[lines];
    int start=0;
    int ln=0;
    for(int i=0;arr[i]!='\0';i++)
    {
        if(arr[i]=='\n')
        {
            fnl[ln]=new char[i-start+1];
            memcpy(fnl[ln],arr+start,i-start);
            fnl[ln][i-start]='\0';
            ln++;
            start=i+1;
        }
    }
    /*std::cout<<"\n";
    for(int i=0;i<ln;i++)
    {
        std::cout<<fnl[i]<<std::endl;
    }*/
    delete[] arr;
    delete l;
    lns=ln;
    return fnl;
}
char** makeparts(char* query)
{
    std::cout<<query<<std::endl;
    int start=0;
    char** parts;
    parts=new char*[3];
    for(int part=0,i=0;query[i]!='\0';i++)
    {
        if(query[i]=='|')
        {
            query[i]='\0';
            parts[part]=query+start;
            start=i+1;
            part++;
        }
    }
    parts[2]=query+start;
    return parts;
}
void handlequery(char** parts,InputArray** allrelations)
{
    /*for(int i=0;i<3;i++)
    {
        std::cout<<parts[i]<<std::endl;
    }*/
    std::cout<<std::endl;
    InputArray** relations=loadrelations(parts[0],allrelations);
    InputArray* result=handlepredicates(relations,parts[1]);
    handleprojection(result,parts[2]);
    std::cout<<std::endl;
    

}
InputArray** loadrelations(char* part,InputArray** allrelations)
{
    std::cout<<"LOADRELATIONS: "<<part<<std::endl;
    int cntr=1;
    uint64_t*** relations;
    for(int i=0;part[i]!='\0';i++)
    {
        if(part[i]==' ')
            cntr++;
    }
    relations=new uint64_t**[cntr];
    //std::cout<<cntr<<" relations"<<std::endl;
}
InputArray* handlepredicates(InputArray** relations,char* part)
{
    std::cout<<"HANDLEPREDICATES: "<<part<<std::endl;

}
void handleprojection(InputArray* array,char* part)
{
    std::cout<<"HANDLEPROJECTION: "<<part<<std::endl;

}

