#include "functions.h"
#include "JobScheduler.h"

void relation::print()
{
    for(uint64_t i=0;i<this->num_tuples;i++)
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
    for (uint64_t i = 0; i < columnsNum; i++) {
        this->columns[i] = new uint64_t[rowsNum];
    }
}

InputArray::InputArray(uint64_t rowsNum) : InputArray(rowsNum, 1) {
    for (uint64_t i = 0; i < rowsNum; i++) {
        columns[0][i] = i;
    }
}

InputArray::~InputArray() {
    for (uint64_t i = 0; i < columnsNum; i++) {
        delete[] this->columns[i];
    }
    delete[] this->columns;
}

InputArray* InputArray::filterRowIds(uint64_t fieldId, int operation, uint64_t numToCompare, InputArray* pureInputArray) {
    InputArray* newInputArrayRowIds = new InputArray(rowsNum);
    uint64_t newInputArrayRowIndex = 0;

    for (uint64_t i = 0; i < rowsNum; i++) {
        uint64_t inputArrayRowId = columns[0][i];
        uint64_t inputArrayFieldValue = pureInputArray->columns[fieldId][inputArrayRowId];
        bool filterApplies = false;
        switch (operation)
        {
        case 0: // '>'
            filterApplies = inputArrayFieldValue > numToCompare;
            break;
        case 1: // '<'
            filterApplies = inputArrayFieldValue < numToCompare;
            break;
        case 2: // '='
            filterApplies = inputArrayFieldValue == numToCompare;
            break;
        default:
            break;
        }

        if (!filterApplies)
            continue;

        newInputArrayRowIds->columns[0][newInputArrayRowIndex++] = inputArrayRowId;
    }

    newInputArrayRowIds->rowsNum = newInputArrayRowIndex; // update rowsNum because the other rows are useless

    return newInputArrayRowIds;
}

InputArray* InputArray::filterRowIds(uint64_t field1Id, uint64_t field2Id, InputArray* pureInputArray) {
    InputArray* newInputArrayRowIds = new InputArray(rowsNum);
    uint64_t newInputArrayRowIndex = 0;

    for (uint64_t i = 0; i < rowsNum; i++) {
        uint64_t inputArrayRowId = columns[0][i];
        uint64_t inputArrayField1Value = pureInputArray->columns[field1Id][inputArrayRowId];
        uint64_t inputArrayField2Value = pureInputArray->columns[field2Id][inputArrayRowId];
        bool filterApplies = inputArrayField1Value == inputArrayField2Value;

        if (!filterApplies)
            continue;

        newInputArrayRowIds->columns[0][newInputArrayRowIndex++] = inputArrayRowId;
    }

    newInputArrayRowIds->rowsNum = newInputArrayRowIndex; // update rowsNum because the other rows are useless
    return newInputArrayRowIds;
}

void InputArray::extractColumnFromRowIds(relation& rel, uint64_t fieldId, InputArray* pureInputArray) {
    rel.num_tuples = rowsNum;
    rel.tuples=new tuple[rel.num_tuples];
    for(uint64_t i = 0; i < rel.num_tuples; i++)
    {
        uint64_t inputArrayRowId = columns[0][i];
        rel.tuples[i].key = inputArrayRowId;
        rel.tuples[i].payload = pureInputArray->columns[fieldId][inputArrayRowId];
    }
}

IntermediateArray::IntermediateArray(uint64_t columnsNum) {
    this->rowsNum = 0;
    this->columnsNum = columnsNum;
    this->results = new uint64_t*[columnsNum];
    for (uint64_t j = 0; j < columnsNum; j++) {
        this->results[j] = NULL;
    }
    this->inputArrayIds = new int[columnsNum];
    this->predicateArrayIds = new int[columnsNum];
}

IntermediateArray::~IntermediateArray() {
    for (uint64_t i = 0; i < columnsNum; i++) {
        if (this->results[i] != NULL) {
            delete[] this->results[i];
        }
    }
    delete[] this->results;
    delete[] this->inputArrayIds;
    delete[] this->predicateArrayIds;
}

void IntermediateArray::extractFieldToRelation(relation* resultRelation, InputArray* inputArray, int predicateArrayId, uint64_t fieldId) {
    resultRelation->num_tuples = rowsNum;
    resultRelation->tuples = new tuple[resultRelation->num_tuples];

    uint64_t columnIndex = findColumnIndexByPredicateArrayId(predicateArrayId);

    for (uint64_t i = 0; i < rowsNum; i++) {
        uint64_t inputArrayRowId = this->results[columnIndex][i];
        
        resultRelation->tuples[i].key = i; // row id of this intermediate array
        resultRelation->tuples[i].payload = inputArray->columns[fieldId][inputArrayRowId];
    }
}

// intermediateResult has always 2 columns and by convention: 
// - if prevIntermediateArray != NULL, then the 1st column of intermediateResult contains row ids of this IntermediateArray and the 2nd column contains row ids of the first-time-used input array
// - if prevIntermediateArray == NULL, then both the 1st and 2nd column of intermediateResult contains row ids of 2 first-time-used input arrays
void IntermediateArray::populate(uint64_t** intermediateResult, uint64_t resultRowsNum, IntermediateArray* prevIntermediateArray, int inputArray1Id, int inputArray2Id, int predicateArray1Id, int predicateArray2Id) {
    this->rowsNum = resultRowsNum;

    for (uint64_t j = 0; j < columnsNum; j++) {
        results[j] = new uint64_t[resultRowsNum];
    }

    if (prevIntermediateArray == NULL) {
        // first time creating an IntermediateArray
        inputArrayIds[0] = inputArray1Id;
        inputArrayIds[1] = inputArray2Id;
        predicateArrayIds[0] = predicateArray1Id;
        predicateArrayIds[1] = predicateArray2Id;
        memcpy(results[0], intermediateResult[0], resultRowsNum*sizeof(uint64_t));
        memcpy(results[1], intermediateResult[1], resultRowsNum*sizeof(uint64_t));
        return;
    }

    memcpy(inputArrayIds, prevIntermediateArray->inputArrayIds, (columnsNum-1)*sizeof(int));
    inputArrayIds[columnsNum - 1] = inputArray2Id;
    memcpy(predicateArrayIds, prevIntermediateArray->predicateArrayIds, (columnsNum-1)*sizeof(int));
    predicateArrayIds[columnsNum - 1] = predicateArray2Id;

    for (uint64_t j = 0; j < columnsNum; j++) {
        for (uint64_t i = 0; i < resultRowsNum; i++) {
            if (j == columnsNum - 1) {
                uint64_t inputArrayRowId = intermediateResult[1][i];
                results[j][i] = inputArrayRowId;
            } else {
                uint64_t prevIntermediateArrayRowId = intermediateResult[0][i];
                results[j][i] = prevIntermediateArray->results[j][prevIntermediateArrayRowId];
            }
        }
        if(j<columnsNum-1)
        {
            delete[] prevIntermediateArray->results[j];
            prevIntermediateArray->results[j]=NULL;
        }
    }
}

bool IntermediateArray::hasInputArrayId(int inputArrayId) {
    for (uint64_t j = 0; j < columnsNum; j++) {
        if (inputArrayIds[j] == inputArrayId) {
            return true;
        }
    }
    return false;
}

void IntermediateArray::print() {
    printf("input array ids: ");
    for (uint64_t j = 0; j < columnsNum; j++) {
        std::cout << inputArrayIds[j] << " ";
    }
    std::cout << std::endl;
    
    printf("predicate array ids: ");
    for (uint64_t j = 0; j < columnsNum; j++) {
        std::cout << predicateArrayIds[j] << " ";
    }
    std::cout << std::endl;

    for (uint64_t i = 0; i < rowsNum; i++) {
        for (uint64_t j = 0; j < columnsNum; j++) {
            std::cout << results[j][i] << " ";
        }
        std::cout << std::endl;
    }
}

uint64_t IntermediateArray::findColumnIndexByInputArrayId(int inputArrayId) {
    for (uint64_t j = 0; j < columnsNum; j++) {
        if (inputArrayIds[j] == inputArrayId)
            return j;
    }
    return -1;
}

uint64_t IntermediateArray::findColumnIndexByPredicateArrayId(int predicateArrayId) {
    for (uint64_t j = 0; j < columnsNum; j++) {
        if (predicateArrayIds[j] == predicateArrayId)
            return j;
    }
    return -1;
}

IntermediateArray* IntermediateArray::selfJoin(int inputArray1Id, int inputArray2Id, uint64_t field1Id, uint64_t field2Id, InputArray* inputArray1, InputArray* inputArray2) {
    IntermediateArray* newIntermediateArray = new IntermediateArray(columnsNum);

    newIntermediateArray->rowsNum = rowsNum;
    for (uint64_t j = 0; j < newIntermediateArray->columnsNum; j++) {
        newIntermediateArray->results[j] = new uint64_t[newIntermediateArray->rowsNum];
    }

    for (uint64_t j = 0; j < newIntermediateArray->columnsNum; j++) {
        newIntermediateArray->inputArrayIds[j] = inputArrayIds[j];
        newIntermediateArray->predicateArrayIds[j] = predicateArrayIds[j];
    }

    uint64_t columnIndexArray1 = findColumnIndexByInputArrayId(inputArray1Id);
    uint64_t columnIndexArray2 = findColumnIndexByInputArrayId(inputArray2Id);

    uint64_t newIntermediateArrayRowIndex = 0;
    for (uint64_t i = 0; i < rowsNum; i++) {
        uint64_t inputArray1RowId = results[columnIndexArray1][i];
        uint64_t inputArray2RowId = results[columnIndexArray2][i];
        uint64_t inputArray1FieldValue = inputArray1->columns[field1Id][inputArray1RowId];
        uint64_t inputArray2FieldValue = inputArray2->columns[field2Id][inputArray2RowId];

        bool filterApplies = inputArray1FieldValue == inputArray2FieldValue;

        if (!filterApplies)
            continue;

        for (uint64_t j = 0; j < columnsNum; j++) {
            newIntermediateArray->results[j][newIntermediateArrayRowIndex] = results[j][i];
        }
        newIntermediateArrayRowIndex++;
    }

    newIntermediateArray->rowsNum = newIntermediateArrayRowIndex; // update rowsNum because the other rows are useless
    return newIntermediateArray;
}

inline uint64_t hashFunction(uint64_t payload, int shift) {
    return (payload >> (8 * shift)) & 0xFF;
}

result* join(relation* R, relation* S,uint64_t**rr,uint64_t**ss,int rsz,int ssz,int joincol)
{
    int64_t samestart=-1;
    int lstsize=1024*1024;
    list*lst=new list(lstsize,2);
    for(uint64_t r=0,s=0,i=0;r<R->num_tuples&&s<S->num_tuples;)
    {
        int64_t dec=R->tuples[r].payload-S->tuples[s].payload;
        if(dec==0)
        {
            lst->insert(R->tuples[r].key);
            lst->insert(S->tuples[s].key);
            i++;
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
            r++;
            continue;
        }
        else
        {
            s++;
            continue;
        }
    }
    result* rslt=new result;
    rslt->lst=lst;
    return rslt;
}

// uint64_t** create_hist(relation *rel, int shift)
// {
//     int x = pow(2,8);
//     uint64_t **hist = new uint64_t*[3];
//     for(int i = 0; i < 3; i++)
//         hist[i] = new uint64_t[x];
//     uint64_t payload;
//     for(int i = 0; i < x; i++)
//     {
//         hist[0][i]= i;
//         hist[1][i]= 0;
//         hist[2][i]= shift;
//     }

//     for (uint64_t i = 0; i < rel->num_tuples; i++)
//     {
//         payload = hashFunction(rel->tuples[i].payload, 7-shift);
//         hist[1][payload]++;
//     }
//     return hist;
// }

// uint64_t** create_psum(uint64_t** hist, uint64_t size)
// {
//     uint64_t count = 0;
//     uint64_t x = size;
//     uint64_t **psum = new uint64_t*[3];
//     for(int i = 0; i < 3; i++)
//         psum[i] = new uint64_t[x];

//     for (uint64_t i = 0; i < x; i++)
//     {
//         psum[0][i] = hist[0][i];
//         psum[1][i] = (uint64_t)count;
//         psum[2][i] = hist[2][i];
//         count+=hist[1][i];
//     }
//     return psum;
// }

// void pr(uint64_t** a, uint64_t array_size)
// {
//     uint64_t i;
//     for (i = 0; i < array_size; i++)
//     {
//         if (a[1][i] != 0 || (i < array_size -1 && a[2][i] < a[2][i+1]))
//         {
//             for (uint64_t l = 0; l < a[2][i]; l++)
//                 std::cout << "  ";
//             std::cout << a[0][i] << ". " << a[1][i] << " - " << a[2][i] << std::endl;
//         }
//     }
// }

// uint64_t** combine_hist(uint64_t** big, uint64_t** small, uint64_t position, uint64_t big_size)   //big_size == size of row in big
// {
//     uint64_t x = pow(2,8), i;    //size of small == pow(2,8)

//     uint64_t **hist = new uint64_t*[3];
//     for(i = 0; i < 3; i++)
//         hist[i] = new uint64_t[x + big_size];

//     /*for (i = 0; i < position; i++) { hist[0][i] = big[0][i]; hist[1][i] = big[1][i]; hist[2][i] = big[2][i]; }*/
//     memcpy(hist[0], big[0], sizeof(big[0][0]) * position);
//     memcpy(hist[1], big[1], sizeof(big[1][0]) * position);
//     memcpy(hist[2], big[2], sizeof(big[2][0]) * position);
//     i = position;
//     hist[0][i] = big[0][i];
//     hist[1][i] = 0;//big[1][i];
//     hist[2][i] = big[2][i];
//     i++;
//     memcpy(&hist[0][i], small[0], sizeof(small[0][0]) * x);
//     memcpy(&hist[1][i], small[1], sizeof(small[1][0]) * x);
//     memcpy(&hist[2][i], small[2], sizeof(small[2][0]) * x);
//     /*for (j = 0; j < x; j++) { hist[0][i] = small[0][j]; hist[1][i] = small[1][j]; hist[2][i] = small[2][j]; i++; }*/
//     memcpy(&hist[0][position + 1 + x], &big[0][position + 1], sizeof(big[0][0]) * (big_size - position-1)); //added -1 and solved some valgrind errors
//     memcpy(&hist[1][position + 1 + x], &big[1][position + 1], sizeof(big[1][0]) * (big_size - position-1));
//     memcpy(&hist[2][position + 1 + x], &big[2][position + 1], sizeof(big[2][0]) * (big_size - position-1));
//     /*for (i = position + 1; i < big_size; i++) { hist[0][i + x] = big[0][i]; hist[1][i + x] = big[1][i]; hist[2][i + x] = big[2][i]; }*/

//     for(i = 0; i < 3; i++)
//     {
//         delete [] big[i];
//         delete [] small[i];
//     }
//     delete [] big;
//     delete [] small;

//     return hist;
// }

// uint64_t find_shift(uint64_t **hist, uint64_t hist_size, uint64_t payload, uint64_t **last)
// {
//     uint64_t i, shift, j, flag;
//     uint64_t x = pow(2, 8);

//     if (last == NULL)
//     {
//         uint64_t** last = new uint64_t*[3];
//         for(i = 0; i < 3; i++)
//             last[i] = new uint64_t[8];
//     }
    
//     for (i = 0; i < hist_size; i++)
//     {
//         //std::cout << payload << ": " << hashFunction(payload, 7 - hist[2][i]) << " : " << hist[0][i] << std::endl;
//         if (i < hist_size - 1 && hist[2][i] < hist[2][i+1])
//         {
//             last[0][hist[2][i]] = hist[0][i];   //hash
//             last[1][hist[2][i]] = hist[2][i];   //shift
//             last[2][hist[2][i]] = (uint64_t)hashFunction(payload, 7 - hist[2][i]) != hist[0][i]; //true or false for hashFunction(payload, 7 - hist[2][i]) != hist[0][i]
//             shift = hist[2][i];
//             if (last[2][hist[2][i]] != 0)
//             {
//                 if (hist[1][i] != 0)
//                 {
//                     flag = 1;
//                     std::cout<<"ok"<<hist[2][i]<<std::endl;
//                     for(j = 0; j < hist[2][i]; j++)
//                     {
//                         if (last[2][hist[2][j]] != 0)//hashFunction(payload, 7 - last[1][j]) != last[0][j])    //last[1] == shift
//                         {
//                             flag = 0;
//                             break;
//                         }
//                     }
//                     if (flag)
//                         return i;
//                     continue;
//                 }
//                 i+=(x-1);
//                 while (hist[2][i+1] > shift)
//                     i++;
                
//             }
//         }

//         if (hist[1][i] != 0 && hashFunction(payload, 7 - hist[2][i]) == hist[0][i])
//         {
//             flag = 1;
//             for(j = 0; j < hist[2][i]; j++)
//             {
//                 if (last[2][hist[2][j]] != 0)//hashFunction(payload, 7 - last[1][j]) != last[0][j])    //last[1] == shift
//                 {
//                     flag = 0;
//                     break;
//                 }
//             }
//             if (flag)
//                 return i;
//         }
//     }
//     //pr(hist, hist_size);
//     std::cout << "NOT FOUND: " << payload << " HASH: " << hashFunction(payload, 7 - 7) << std::endl;
//     return 0;
// }

// void print_psum_hist(uint64_t** psum, uint64_t** hist, int array_size)
// {
//     std::cout << "<<<<<<<" << std::endl;
//     for (int i = 0; i < array_size; i++)
//         if (i == array_size-1 || psum[1][i] != psum[1][i+1])
//             std::cout << psum[0][i] << " " << psum[1][i] << " " << psum[2][i] << std::endl;
//     std::cout << "<<<<<<<" << std::endl;
//     pr(hist, array_size);
//     std::cout << "<<<<<<<" << std::endl;
// }
uint64_t* psumcreate(uint64_t* hist)
{
    uint64_t count = 0;
    uint64_t *psum = new uint64_t[power]{0};

    for (uint64_t i = 0; i < power; i++)
    {
        psum[i] = (uint64_t)count;
        count+=hist[i];
    }
    return psum;
}

uint64_t* histcreate(tuple* array,int offset,int shift)
{
    uint64_t *hist = new uint64_t[power]{0};
    for (uint64_t i = 0; i < offset; i++)
        hist[hashFunction(array[i].payload, 7-shift)]++;

    return hist;
}

void tuplereorder_serial(tuple* array,tuple* array2, int offset,int shift)
{
    uint64_t* hist=histcreate(array,offset,shift);
    uint64_t* psum=psumcreate(hist);
    for(int i=0;i<offset;i++)
    {
        uint64_t hash=hashFunction(array[i].payload,7-shift);
        memcpy(array2+psum[hash],array+i,sizeof(tuple));
        psum[hash]++;
    }
    memcpy(array,array2,offset*sizeof(tuple));
    for(int i=0,start=0;i<power;i++)
    {
        if(hist[i]==0)
            continue;
        if(hist[i] > TUPLES_PER_BUCKET && shift < 7)
            tuplereorder_serial(array+start,array2+start,psum[i]-start,shift+1); //psum[i]-start = endoffset
        else            
            quickSort(array,start, psum[i]-1);

        start=psum[i];
    }
    delete[] psum;
    delete[] hist;
}

void tuplereorder_parallel(tuple* array,tuple* array2, int offset,int shift)
{
    uint64_t* hist=histcreate(array,offset,shift);
    uint64_t* psum=psumcreate(hist);
    for(int i=0;i<offset;i++)
    {
        uint64_t hash=hashFunction(array[i].payload,7-shift);
        memcpy(array2+psum[hash],array+i,sizeof(tuple));
        psum[hash]++;
    }
    memcpy(array,array2,offset*sizeof(tuple));
    for(int i=0,start=0;i<power;i++)
    {
        if(hist[i]==0)
            continue;
        if(hist[i] > TUPLES_PER_BUCKET && shift < 7)
            tuplereorder_parallel(array+start,array2+start,psum[i]-start,shift+1); //psum[i]-start = endoffset
        else            
            quickSort(array,start, psum[i]-1);

        start=psum[i];
    }
    delete[] psum;
    delete[] hist;
}

void tuplereorder(tuple* array,tuple* array2, int offset,int shift, Type t)
{
    if (t == serial)
        tuplereorder_serial(array, array2, offset, shift);
    else if (t == parallel)
    {
        JobScheduler scheduler(4, 10);
        tuplereorder_parallel(array, array2, offset, shift);
        scheduler.~JobScheduler();
    }
}

// relation* re_orderedd(relation *rel, relation* new_rel, int no_used)
// {
//     int shift = 0;
//     uint64_t x = pow(2, 8), array_size = x;
//     //create histogram
//     uint64_t** hist = create_hist(rel, shift), **temp_hist = NULL;
//     //create psum
//     uint64_t** psum = create_psum(hist, x);
//     uint64_t payload;
//     uint64_t i, j, y;
//     bool clear;
//     uint64_t** arr = new uint64_t*[3];
//     for(i = 0; i < 3; i++)
//         arr[i] = new uint64_t[8];

//     uint64_t** tempPsum = new uint64_t*[3];
//     for (uint64_t i = 0; i < 3; i++) {
//         tempPsum[i] = new uint64_t[x];
//         memcpy(tempPsum[i], psum[i], x*sizeof(uint64_t));
//     }
    
//     i = 0;
//     while(i < rel->num_tuples)
//     {
//         payload = hashFunction(rel->tuples[i].payload, 7 - shift);
//         //find hash in psum = pos in new relation
//         new_rel->tuples[tempPsum[1][payload]].payload = rel->tuples[i].payload;
//         new_rel->tuples[tempPsum[1][payload]++].key = rel->tuples[i].key;
//         i++;
//     }

//     clear = false; //make a full loop with clear == false to end
//     i = 0;
//     while (i < array_size)
//     {
//         if ((hist[1][i] > TUPLES_PER_BUCKET) && (hist[2][i] < 7))
//         {
//             clear = true;
//             //new relation from psum[1][i] to psum[1][i+1]
//             if (rel == NULL)
//                 rel = new relation();
//             uint64_t first = psum[1][i];
//             uint64_t last = new_rel->num_tuples;
//             if (i != array_size - 1)
//                 last = psum[1][i+1];
//             rel->num_tuples = last - first;
//             if(rel->tuples == NULL)
//                 rel->tuples = new tuple[new_rel->num_tuples];
            
//             memcpy(rel->tuples, new_rel->tuples + first, rel->num_tuples*sizeof(tuple));
            
//             temp_hist = create_hist(rel, hist[2][i] + 1);
            
//             hist = combine_hist(hist, temp_hist, i, array_size);
//             array_size+=x;

//             delete [] psum[0];
//             delete [] psum[1];
//             delete [] psum[2];
//             delete [] psum;
//             psum = create_psum(hist, array_size);

//             j = 0;
//             if (rel == NULL)
//                 rel = new relation();
//             if (sizeof(*rel->tuples) != sizeof(*new_rel->tuples))
//             {
//                 delete [] rel->tuples;
//                 rel->tuples = new tuple[new_rel->num_tuples];
//             }
//             rel->num_tuples = new_rel->num_tuples;

//             for (uint64_t i = 0; i < 3; i++) {
//                 delete[] tempPsum[i];
//                 tempPsum[i] = new uint64_t[array_size];
//                 memcpy(tempPsum[i], psum[i], array_size*sizeof(uint64_t));
//             }
            
//             while(j < new_rel->num_tuples)
//             {
//                 //hash
//                 payload = find_shift(hist, array_size, new_rel->tuples[j].payload, arr);
//                 //find hash in psum = pos in new relation
//                 rel->tuples[tempPsum[1][payload]].payload = new_rel->tuples[j].payload;
//                 rel->tuples[tempPsum[1][payload]++].key = new_rel->tuples[j].key;
//                 j++;
//             }


//             tuple *temp_tuple;
//             temp_tuple = rel->tuples;
//             rel->tuples = new_rel->tuples;
//             new_rel->tuples = temp_tuple;
//             j = rel->num_tuples;
//             rel->num_tuples = new_rel->num_tuples;
//             new_rel->num_tuples = j;

//             i+=(x-1);   //NOT SURE
//         }
        
//         if (hist[1][i] <= TUPLES_PER_BUCKET || hist[2][i] > 7)
//         {
//             if (hist[1][i] > 0)
//             {
//                 if (i + 1 < array_size)
//                     sortBucket(new_rel, psum[1][i], psum[1][i+1] - 1);
//                 else
//                     sortBucket(new_rel, psum[1][i], rel->num_tuples - 1);
//                 new_rel->print();
//             }
//         }

//         i++;
//         if (i == array_size && clear)
//         {
//             i = 0;
//             clear = false;
//         }
//     }
//     //testing
//     //print_psum_hist(psum, hist, array_size);

//     for (uint64_t i = 0; i < 3; i++) {
//         delete[] tempPsum[i];
//     }
//     delete[] tempPsum;
    
//     delete [] hist[0];
//     delete [] hist[1];
//     delete [] hist[2];
//     delete [] hist;
    
//     delete [] psum[0];
//     delete [] psum[1];
//     delete [] psum[2];
//     delete [] psum;

//     delete [] arr[0];
//     delete [] arr[1];
//     delete [] arr[2];

//     delete [] arr;

//     return new_rel;
// }

uint64_t psum_create(bucket *B, uint64_t count)
{
    B->hist->psum = new uint64_t[power]{0};
    for (uint64_t i = 0; i < power; i++)
    {
        if (B->hist->hist[i] != 0)
        {
            if (B->hist->next[i] == NULL)
            {
                B->hist->psum[i] = count;
                count+=B->hist->hist[i];
            }
            else
            {
                count = psum_create(B->hist->next[i], count);
            }
        }
    }
    return count;
}

void call_quicksort(bucket *B, relation *rel)
{
    int64_t first = 0;
    B->index = 0;
    while(B->index < power)
    {
        if (B->hist->next[B->index] != NULL)
        {
            B = B->hist->next[B->index];
            B->index = 0;
            continue;
        }
        if (B->hist->psum[B->index] > 0)
        {
            
            if (B->index + 1 < power)
            {
                //std::cout << first << " " << B->hist->psum[B->index] - 1 << std::endl;
                quickSort(rel->tuples, first, B->hist->psum[B->index] - 1);
                first = B->hist->psum[B->index];
            }
            if (B->prev == NULL)
            {
                //std::cout << first << " " << rel->num_tuples - 1 << std::endl;
                quickSort(rel->tuples, first, rel->num_tuples - 1);
            }
        }

        if (B->index == power - 1 && B->prev != NULL)
            B = B->prev;
        B->index++;
    }
}

bucket::~bucket() {
        rel->~relation();
        hist->~histogram();
}

histogram::~histogram() {
    delete [] hist;
    delete [] psum;
    for (int i = 0; i < power; i++)
    {
        if (next[i] != NULL)
            next[i]->~bucket();
    }
    delete [] next;
}

relation* re_ordered_2(relation *rel, relation* new_rel, int no_used)
{
    bucket *root = new bucket(), *B = NULL;
    B = root;
    B->rel = rel;
    B->hist = new histogram();
    B->shift = 0;
    B->prev = NULL;
    B->hist->hist = new uint64_t[power]{0};
    B->hist->next = new bucket*[power]();
    //create hist
    for (uint64_t i = 0; i < B->rel->num_tuples; i++)
        B->hist->hist[hashFunction(B->rel->tuples[i].payload, 7 - B->shift)]++;
    B->index = 0;
    while (B->index < power)
    {
        if (B->hist->hist[B->index] > TUPLES_PER_BUCKET && B->shift < 7)
        {
            B->hist->next[B->index] = new bucket();
            B->hist->next[B->index]->rel = new relation();
            B->hist->next[B->index]->rel->num_tuples = B->hist->hist[B->index];
            B->hist->next[B->index]->rel->tuples = new tuple[B->hist->next[B->index]->rel->num_tuples];
            B->hist->next[B->index]->hist = new histogram();
            B->hist->next[B->index]->shift = B->shift + 1;
            B->hist->next[B->index]->hist->hist = new uint64_t[power]{0};
            B->hist->next[B->index]->hist->next = new bucket*[power]();
            B->hist->next[B->index]->index = 0;
            B->hist->next[B->index]->prev = B;
            //transfer tuples of this bucket
            int pos = 0;
            for (uint64_t j = 0; j < power; j++)
            {
                if (hashFunction(B->rel->tuples[j].payload, 7 - B->shift) == B->index)
                {
                    B->hist->next[B->index]->rel->tuples[pos] = B->rel->tuples[j];
                    pos++;
                    if (pos >= B->hist->next[B->index]->rel->num_tuples)
                        break;
                }
            }
            B->hist->hist[B->index] == 0;
            B = B->hist->next[B->index];
            //create hist
            for (uint64_t i = 0; i < B->rel->num_tuples; i++)
            {
                B->hist->hist[hashFunction(B->rel->tuples[i].payload, 7 - B->shift)]++;
            }
            continue;
        }
        if (B->index == power - 1 && B->prev != NULL)
            B = B->prev;
         B->index++;
    }
    B = root;
    psum_create(B, 0); 
    uint64_t hash, payload;
    B = root;
    for (int i = 0; i < rel->num_tuples; i++)
    {
        payload = rel->tuples[i].payload;
        while(B->hist->next[hash = hashFunction(payload,7-B->shift)] != NULL)
        {
            B = B->hist->next[hash];
            hash = hashFunction(payload,7-B->shift);
        }
        new_rel->tuples[B->hist->psum[hash]].key = rel->tuples[i].key;
        new_rel->tuples[B->hist->psum[hash]++].payload = rel->tuples[i].payload;
        B = root;
    }
    B = root;
    call_quicksort(B, new_rel);
    delete root->hist;
    return new_rel;
}

void mid_func(tuple *t1, tuple *t2, int num, int not_used)
{
    relation *new_rel_R = new relation(), *R = new relation();
    new_rel_R->num_tuples=num;
    new_rel_R->tuples = new tuple[num];
    R->num_tuples=num;
    R->tuples = new tuple[num];

    //memcpy(new_rel_R->tuples, t2, num*sizeof(tuple));
    memcpy(R->tuples, t1, num*sizeof(tuple));

    new_rel_R = re_ordered_2(R, new_rel_R, not_used);
    memcpy(t1, new_rel_R->tuples, num*sizeof(tuple));

    R->~relation();
    new_rel_R->~relation();
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
    //std::cout<<"sort "<<startIndex<<" "<<stopIndex<<std::endl;
    quickSort(rel->tuples, startIndex, stopIndex);
}

InputArray** readArrays() {
    InputArray** inputArrays = new InputArray*[MAX_INPUT_ARRAYS_NUM]; // size is fixed
    size_t fileNameSize = MAX_INPUT_FILE_NAME_SIZE;
    char fileName[fileNameSize];
    ssize_t rtn;

    unsigned int inputArraysIndex = 0;
    while (fgets(fileName, fileNameSize, stdin) != NULL) {
        fileName[strlen(fileName) - 1] = '\0'; // remove newline character
        if (strcmp(fileName, "Done") == 0)
            break;

        uint64_t rowsNum, columnsNum;
        FILE *fileP;

        fileP = fopen(fileName, "rb");
        if (fileP == NULL)
        {
            printf("Could Not Open File (%s)\n", fileName);
            return NULL;
        }

        if ((rtn = fread(&rowsNum, sizeof(uint64_t), 1, fileP)) < 0) 
        {
            printf("fread for file <%s> returned %ld\n", fileName, rtn);
            return NULL;
        }
        if ((rtn = fread(&columnsNum, sizeof(uint64_t), 1, fileP)) < 0)
        {
            printf("fread for file <%s> returned %ld\n", fileName, rtn);
            return NULL;
        }

        inputArrays[inputArraysIndex] = new InputArray(rowsNum, columnsNum);

        for (uint64_t i = 0; i < columnsNum; i++) {
            for (uint64_t j = 0; j < rowsNum; j++) {
                if ((rtn = fread(&inputArrays[inputArraysIndex]->columns[i][j], sizeof(uint64_t), 1, fileP)) < 0)
                {
                    printf("fread for file <%s> returned %ld\n", fileName, rtn);
                    return NULL;
                }
            }
        }

        fclose(fileP);

        inputArraysIndex++;
    }

    return inputArrays;
}

void InputArray::print() {
    for (uint64_t i = 0; i < rowsNum; i++) {
        for (uint64_t j = 0; j < columnsNum; j++) {
            std::cout << columns[j][i] << " ";
        }
        std:: cout << std::endl;
    }
    std:: cout << std::endl;
}

char** readbatch(int& lns)
{
    lns=0;
    char ch;
    list* l=new list(1024,0);
    int flag=0;
    int lines=0;
    while(1)
    {
        ch=getchar();
        if(ch==EOF)
        {
            delete l;
            return NULL;
        }
        if(ch=='\n'&&flag)
            continue;
        l->insert(ch);
        if(ch=='F'&&flag)
        {
            while(1)
            {
                ch=getchar();
                if(ch=='\n')
                    break;
                if(ch==EOF)
                    break;
            }
            break;
        }
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
    delete[] arr;
    delete l;
    lns=ln;    
    return fnl;
}
char** makeparts(char* query)
{
    //std::cout<<query<<std::endl;
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
    int relationIds[MAX_INPUT_ARRAYS_NUM];
    int relationsnum;
    loadrelationIds(relationIds, parts[0], relationsnum);
    IntermediateArray* result=handlepredicates(allrelations,parts[1],relationsnum, relationIds);
    handleprojection(result,allrelations,parts[2], relationIds);
    if(result!=NULL)
        delete result;
    delete[] parts;   

}
void loadrelationIds(int* relationIds, char* part, int& relationsnum)
{
    // std::cout<<"LOADRELATIONS: "<<part<<std::endl;
    int cntr=1;
    uint64_t*** relations;
    for(int i=0;part[i]!='\0';i++)
    {
        if(part[i]==' ')
            cntr++;
    }

    char tempPart[strlen(part) + 1];
    strcpy(tempPart, part);
    int i = 0;
    char* token = strtok(tempPart, " ");
    while (token) {
        relationIds[i++] = atoi(token);
        token = strtok(NULL, " ");
    }
    relationsnum=cntr;
}

bool shouldSort(uint64_t** predicates, int predicatesNum, int curPredicateIndex, int curPredicateArrayId, int curFieldId, bool prevPredicateWasFilterOrSelfJoin) {
    if (curPredicateIndex <= 0 || prevPredicateWasFilterOrSelfJoin)
        return true;

    uint64_t* prevPredicate = predicates[curPredicateIndex - 1];

    int prevPredicateArray1Id = prevPredicate[0], prevPredicateArray2Id = prevPredicate[3],
         prevField1Id = prevPredicate[1], prevField2Id = prevPredicate[4];

    return !((curPredicateArrayId == prevPredicateArray1Id && curFieldId == prevField1Id) ||
            (curPredicateArrayId == prevPredicateArray2Id && curFieldId == prevField2Id));
}
bool isRelationOrdered(relation &rel) {
    for (int i = 0; i < rel.num_tuples - 1; i++) {
        if (rel.tuples[i].payload > rel.tuples[i + 1].payload)
            return false;
    }

    return true;
}
IntermediateArray* handlepredicates(InputArray** inputArrays,char* part,int relationsnum, int* relationIds)
{
    int cntr;
    uint64_t** preds=splitpreds(part,cntr);
    preds=optimizepredicates(preds,cntr,relationsnum,relationIds);

    InputArray** inputArraysRowIds = new InputArray*[relationsnum];
    for (int i = 0; i < relationsnum; i++) {
        inputArraysRowIds[i] = new InputArray(inputArrays[relationIds[i]]->rowsNum);
    }

    IntermediateArray* curIntermediateArray = NULL;
    bool prevPredicateWasFilterOrSelfJoin = false;

    for(int i=0;i<cntr;i++)
    {
        bool isFilter = preds[i][3] == (uint64_t) - 1;
        int predicateArray1Id = preds[i][0];
        int predicateArray2Id = preds[i][3];
        int inputArray1Id = relationIds[predicateArray1Id];
        int inputArray2Id = isFilter ? -1 : relationIds[predicateArray2Id];
        
        InputArray* inputArray1 = inputArrays[inputArray1Id];
        InputArray* inputArray2 = isFilter ? NULL : inputArrays[inputArray2Id];
        InputArray* inputArray1RowIds = inputArraysRowIds[predicateArray1Id];
        InputArray* inputArray2RowIds = isFilter ? NULL : inputArraysRowIds[predicateArray2Id];

        uint64_t field1Id = preds[i][1];
        uint64_t field2Id = preds[i][4];
        int operation = preds[i][2];
        if (isFilter) {
            uint64_t numToCompare = field2Id;
            InputArray* filteredInputArrayRowIds = inputArray1RowIds->filterRowIds(field1Id, operation, numToCompare, inputArray1);
            delete inputArray1RowIds;
            inputArraysRowIds[predicateArray1Id] = filteredInputArrayRowIds;
            prevPredicateWasFilterOrSelfJoin = true;
            continue;
        }

        switch (operation)
        {
        case 2: // '='
                if (predicateArray1Id == predicateArray2Id) {
                    // self-join of InputArray
                    InputArray* filteredInputArrayRowIds = inputArray1RowIds->filterRowIds(field1Id, field2Id, inputArray1);
                    delete inputArray1RowIds;
                    inputArraysRowIds[predicateArray1Id] = filteredInputArrayRowIds;
                    prevPredicateWasFilterOrSelfJoin = true;
                    continue;
                }

                if (inputArray1Id != inputArray2Id && 
                    (curIntermediateArray != NULL && curIntermediateArray->hasInputArrayId(inputArray1Id)
                    && curIntermediateArray != NULL && curIntermediateArray->hasInputArrayId(inputArray2Id))) {
                    // self-join of IntermediateArray
                    IntermediateArray* filteredIntermediateArray = curIntermediateArray->selfJoin(inputArray1Id, inputArray2Id, field1Id, field2Id, inputArray1, inputArray2);
                    delete curIntermediateArray;
                    curIntermediateArray = filteredIntermediateArray;
                    prevPredicateWasFilterOrSelfJoin = true;
                    continue;
                }

                {
                    relation rel1, rel2;
                    bool rel2ExistsInIntermediateArray = false;

                    // fill rel1
                    if (curIntermediateArray == NULL || !curIntermediateArray->hasInputArrayId(inputArray1Id)) {
                        inputArray1RowIds->extractColumnFromRowIds(rel1, field1Id, inputArray1);
                    } else {
                        curIntermediateArray->extractFieldToRelation(&rel1, inputArray1, predicateArray1Id, field1Id);
                    }

                    // fill rel2
                    if (curIntermediateArray == NULL || inputArray1Id == inputArray2Id || !curIntermediateArray->hasInputArrayId(inputArray2Id)) {
                        inputArray2RowIds->extractColumnFromRowIds(rel2, field2Id, inputArray2);
                    } else {
                        rel2ExistsInIntermediateArray = true;
                        curIntermediateArray->extractFieldToRelation(&rel2, inputArray2, predicateArray2Id, field2Id);
                    }

                    relation* newRel1 = new relation();
                    relation* reorderedRel1=&rel1;
                    
                    if (shouldSort(preds, cntr, i, predicateArray1Id, field1Id, prevPredicateWasFilterOrSelfJoin)) {
                        tuple* t=new tuple[rel1.num_tuples];
                        tuplereorder(rel1.tuples,t,rel1.num_tuples,0, parallel);
                        //mid_func(rel1.tuples,t,rel1.num_tuples,0);

                        delete[] t;
                    }
                    
                    relation* newRel2 = new relation();
                    relation* reorderedRel2=&rel2;
                    
                    if (shouldSort(preds, cntr, i, predicateArray2Id, field2Id, prevPredicateWasFilterOrSelfJoin)) {
                        tuple* t=new tuple[rel2.num_tuples];
                        tuplereorder(rel2.tuples,t,rel2.num_tuples,0, parallel);
                        //mid_func(rel2.tuples,t,rel2.num_tuples,0);
                        delete[] t;
                    }
                    
                    result* rslt = join(rel2ExistsInIntermediateArray ? reorderedRel2 : reorderedRel1, rel2ExistsInIntermediateArray ? reorderedRel1 : reorderedRel2, inputArray1->columns, inputArray2->columns, inputArray1->columnsNum, inputArray2->columnsNum, 0);
                    if (rslt->lst->rows == 0) {
                        // no results
                        for(int i=0;i<cntr;i++)
                        {
                            delete[] preds[i];
                        }
                        delete[] preds;
                        for(int i=0;i<relationsnum;i++)
                            delete inputArraysRowIds[i];
                        delete[] inputArraysRowIds;
                        delete newRel1;
                        delete newRel2;
                        delete rslt->lst;
                        delete rslt;
                        return NULL;
                    }
                    uint64_t** resultArray=rslt->lst->lsttoarr();
                    uint64_t rows=rslt->lst->rows;
                    uint64_t rowsz=rslt->lst->rowsz;
                    delete rslt->lst;
                    delete rslt;
                    delete newRel1;
                    delete newRel2;                    
                    
                    if (curIntermediateArray == NULL) {
                        // first join
                        curIntermediateArray = new IntermediateArray(2);
                        curIntermediateArray->populate(resultArray, rows, NULL, inputArray1Id, inputArray2Id, predicateArray1Id, predicateArray2Id);
                    } else {
                        IntermediateArray* newIntermediateArray = new IntermediateArray(curIntermediateArray->columnsNum + 1);
                        newIntermediateArray->populate(resultArray, rows, curIntermediateArray, -1, rel2ExistsInIntermediateArray ? inputArray1Id : inputArray2Id, predicateArray1Id, predicateArray2Id);
                        delete curIntermediateArray;
                        curIntermediateArray = newIntermediateArray;
                    }
                    prevPredicateWasFilterOrSelfJoin = false;

                    for(int i=0;i<rowsz;i++)
                        delete[] resultArray[i];
                    delete[] resultArray;
                }
            break;
        default:
            break;
        }
        
        /*******TO ANTONIS******************/
        //kathe grammi edo einai ena predicate olokliro
        //preds[i][0]=sxesi1
        //preds[i][1]=stili1
        //preds[i][2]=praxi opou
            //0  einai to >
            //1  einai to <
            //2  einai to =
        //preds[i][3]=sxesi2
            //sigrine me (uint64_t)-1 to opoio bgazei 18446744073709551615 kai theoroume an einai isa tote exoume filtro
        //preds[i][4]=stili2 
            //opou an h sxesi 2 einai isi me -1 opos eipa apo pano tote to stili 2 periexei to filtro
        /***********END***************************/
    }

    for(int i=0;i<cntr;i++)
    {
        delete[] preds[i];
    }
    delete[] preds;
    for(int i=0;i<relationsnum;i++)
        delete inputArraysRowIds[i];
    delete[] inputArraysRowIds;
    return curIntermediateArray != NULL && curIntermediateArray->rowsNum > 0 ? curIntermediateArray : NULL;



}
void handleprojection(IntermediateArray* rowarr,InputArray** array,char* part, int* relationIds)
{
    // std::cout<<"HANDLEPROJECTION: "<<part<<std::endl;
    int projarray,projcolumn,predicatearray;
    for(int i=0,start=0;(i==0)||(i>0&&part[i-1])!='\0';i++)
    {
        if(part[i]=='.')
        {
            part[i]='\0';
            projarray=relationIds[atoi(part+start)];
            predicatearray=atoi(part+start);
            part[i]='.';
            start=i+1;
        }
        if(part[i]==' '||part[i]=='\0')
        {
            int flg=0;
            if(part[i]==' ')
            {
                part[i]='\0';
                flg=1;
            }
            projcolumn=atoi(part+start);
            if(flg)
                part[i]=' ';
            start=i+1;
            
            uint64_t sum=0;
            if(rowarr!=NULL)
            {
                uint64_t key;
                key=rowarr->predicateArrayIds[predicatearray];
                for(uint64_t i =0;i<rowarr->rowsNum;i++)
                {
                    sum+=array[projarray]->columns[projcolumn][rowarr->results[key][i]];
                }
            }
            //std::cout<<projarray<<"."<<projcolumn<<": ";
            if(sum!=0)
                std::cout<<sum;
            else
                std::cout<<"NULL";
            if(part[i]!='\0')
                std::cout<<" ";
        }
    }
}
uint64_t** splitpreds(char* ch,int& cn)
{
    int cntr=1;
    for(int i=0;ch[i]!='\0';i++)
    {
        if(ch[i]=='&')
            cntr++;
    }
    uint64_t** preds;
    preds=new uint64_t*[cntr];
    for(int i=0;i<cntr;i++)
    {
        preds[i]=new uint64_t[5];
    }
    cntr=0;
    int start=0;
    for(int i=0;ch[i]!='\0';i++)
    {
        if(ch[i]=='&')
        {
            ch[i]='\0';
            predsplittoterms(ch+start,preds[cntr][0],preds[cntr][1],preds[cntr][3],preds[cntr][4],preds[cntr][2]);
            cntr++;
            start=i+1;
        }
    }
    predsplittoterms(ch+start,preds[cntr][0],preds[cntr][1],preds[cntr][3],preds[cntr][4],preds[cntr][2]);
    cntr++;
    cn=cntr;
    return preds;
}
bool notin(uint64_t** check, uint64_t* in, int cntr)
{
    if (check==NULL || in==NULL)
        return true;
    for(int j=0;j<cntr;j++)
    {
        if(check[j]!=NULL && check[j]==in)
            return false;
    }
    
    return true;
}
uint64_t** optimizepredicates(uint64_t** preds,int cntr,int relationsnum,int* relationIds)
{
    uint64_t** result=new uint64_t*[cntr];
    int place=0;
    for(int i=0;i<relationsnum;i++)
    {
        for(int j=0;j<cntr;j++)
        {
            if(preds[j][0]==i&&(preds[j][3]==(uint64_t)-1||preds[j][3]==i))
            {
                if(notin(result,preds[j],place))
                {
                    result[place]=preds[j];
                    place++;
                }
            }
        }
    }
    for(int i=0;i<relationsnum;i++)
    {
        for(int j=0;j<cntr;j++)
        {
            if(preds[j][0]==i)
            {
                if(notin(result,preds[j],place))
                {
                    result[place]=preds[j];
                    place++;
                }
            }
        }
    }
    delete[] preds;
    return result;
    


}

void predsplittoterms(char* pred,uint64_t& rel1,uint64_t& col1,uint64_t& rel2,uint64_t& col2,uint64_t& flag)
{
    char buffer[1024];
    rel1=col1=rel2=col2=flag=-1;
    for(int start=0,end=0,i=0,j=0,term=0;(i==0)||(i>0&&pred[i-1])!='\0';i++,j++)
    {
        if(pred[i]=='.')
        {
            buffer[j]='\0';
            if(term==0)
                rel1=atoll(buffer);
            else
                rel2=atoll(buffer);
            j=-1;
            term++;
        }
        else if(pred[i]=='>'||pred[i]=='<'||pred[i]=='=')
        {
            if(pred[i]=='>')
                flag=0;
            else if(pred[i]=='<')
                flag=1;
            else if(pred[i]=='=')
                flag=2;
            buffer[j]='\0';
            col1=atoll(buffer);
            term++;
            j=-1;
        }
        else if(pred[i]=='\0')
        {
            buffer[j]='\0';
            col2=atoll(buffer);
        }
        else
            buffer[j]=pred[i];

    }
}

