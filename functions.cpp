#include "functions.h"
#include "JobScheduler.h"
#include <unistd.h>

RunningMode queryMode = serial;
RunningMode reorderMode = serial;
RunningMode quickSortMode = serial;
RunningMode joinMode = serial;
RunningMode projectionMode=serial;
RunningMode filterMode = serial;
bool newJobPerBucket = false;
bool OptimizePredicatesFlag=false;

pthread_mutex_t* predicateJobsDoneMutexes;
pthread_cond_t* predicateJobsDoneConds;
pthread_cond_t *jobsCounterConds;
pthread_mutex_t queryJobDoneMutex;
pthread_cond_t queryJobDoneCond;
int available_threads;

bool** lastJobDoneArrays;
// bool* queryJobDoneArray;
int queryJobDone;
char** QueryResult;
JobScheduler *scheduler=NULL;

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

void ColumnStats::calculateDistinctValuesNum(const InputArray* inputArray, uint64_t columnIndex) {
    uint64_t boolArraySize = maxValue - minValue + 1;
    bool arraySizeCut = false;
    if (boolArraySize > MAX_BOOLEAN_ARRAY_SIZE) {
        boolArraySize = MAX_BOOLEAN_ARRAY_SIZE;
        arraySizeCut = true;
    }
    distinctValuesNum = valuesNum;
    bool* boolArray=new bool[boolArraySize]{false};

    for (uint64_t j = 0; j < inputArray->rowsNum; j++) {
        uint64_t boolArrayIndex = inputArray->columns[columnIndex][j] - minValue;
        if (arraySizeCut) {
            boolArrayIndex %= MAX_BOOLEAN_ARRAY_SIZE;
        }

        if (!boolArray[boolArrayIndex]) {
            boolArray[boolArrayIndex] = true;
        } else {
            distinctValuesNum--;
        }
    }
    delete[] boolArray;
}

InputArray::InputArray(uint64_t rowsNum, uint64_t columnsNum) {
    this->rowsNum = rowsNum;
    this->columnsNum = columnsNum;
    this->columns = new uint64_t*[columnsNum];
    for (uint64_t i = 0; i < columnsNum; i++) {
        this->columns[i] = new uint64_t[rowsNum];
    }
    columnsStats = NULL;
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

    if (columnsStats != NULL) {
        delete[] columnsStats;
    }
}

InputArray* InputArray::filterRowIds(uint64_t fieldId, int operation, uint64_t numToCompare, const InputArray* pureInputArray, uint64_t startIndex, uint64_t stopIndex) {
    InputArray* newInputArrayRowIds = new InputArray(stopIndex - startIndex);

    uint64_t newInputArrayRowIndex = 0;
    for (uint64_t i = startIndex; i < stopIndex; i++) {
        uint64_t inputArrayRowId = columns[0][i];
        // std::cout<<"inputArrayRowId: "<<inputArrayRowId<<std::endl;

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

// InputArray* InputArray::filterRowIds(uint64_t fieldId, int operation, uint64_t numToCompare, const InputArray* pureInputArray, uint64_t startIndex, uint64_t stopIndex) {
//     InputArray* newInputArrayRowIds = new InputArray(rowsNum);
//     uint64_t newInputArrayRowIndex = 0;

//     for (uint64_t i = startIndex; i < stopIndex; i++) {
//         uint64_t inputArrayRowId = columns[0][i];
//         // std::cout<<"inputArrayRowId: "<<inputArrayRowId<<std::endl;

//         uint64_t inputArrayFieldValue = pureInputArray->columns[fieldId][inputArrayRowId];
//         bool filterApplies = false;
//         switch (operation)
//         {
//         case 0: // '>'
//             filterApplies = inputArrayFieldValue > numToCompare;
//             break;
//         case 1: // '<'
//             filterApplies = inputArrayFieldValue < numToCompare;
//             break;
//         case 2: // '='
//             filterApplies = inputArrayFieldValue == numToCompare;
//             break;
//         default:
//             break;
//         }

//         if (!filterApplies)
//             continue;

//         newInputArrayRowIds->columns[0][newInputArrayRowIndex++] = inputArrayRowId;
//     }

//     newInputArrayRowIds->rowsNum = newInputArrayRowIndex; // update rowsNum because the other rows are useless
// }

InputArray* InputArray::filterRowIds(uint64_t field1Id, uint64_t field2Id, const InputArray* pureInputArray, uint64_t startIndex, uint64_t stopIndex) {
    InputArray* newInputArrayRowIds = new InputArray(stopIndex - startIndex);
    uint64_t newInputArrayRowIndex = 0;

    for (uint64_t i = startIndex; i < stopIndex; i++) {
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

void InputArray::extractColumnFromRowIds(relation& rel, uint64_t fieldId, const InputArray* pureInputArray) {
    rel.num_tuples = rowsNum;
    rel.tuples=new tuple[rel.num_tuples];
    for(uint64_t i = 0; i < rel.num_tuples; i++)
    {
        uint64_t inputArrayRowId = columns[0][i];
        rel.tuples[i].key = inputArrayRowId;
        rel.tuples[i].payload = pureInputArray->columns[fieldId][inputArrayRowId];
    }
}

void InputArray::initStatistics() {
    columnsStats = new ColumnStats[columnsNum];
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

void IntermediateArray::extractFieldToRelation(relation* resultRelation, const InputArray* inputArray, int predicateArrayId, uint64_t fieldId) {
    resultRelation->num_tuples = rowsNum;
    resultRelation->tuples = new tuple[resultRelation->num_tuples];
    // std::cout<<"pred arr id: "<<predicateArrayId<<std::endl;
    uint64_t columnIndex = findColumnIndexByPredicateArrayId(predicateArrayId);
    // std::cout<<"index: "<<columnIndex<<std::endl;
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

bool IntermediateArray::hasPredicateArrayId(int predicateArrayId) {
    for (uint64_t j = 0; j < columnsNum; j++) {
        if (predicateArrayIds[j] == predicateArrayId) {
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

IntermediateArray* IntermediateArray::selfJoin(int inputArray1Id, int inputArray2Id, uint64_t field1Id, uint64_t field2Id, const InputArray* inputArray1, const InputArray* inputArray2) {
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

InputArray* combineInputArrayRowIds(InputArray** inputArrayRowIdsParts, int partsNum) {
    uint64_t totalPartsRowsNum = 0;
    for (int i = 0; i < partsNum; i++) {
        totalPartsRowsNum += inputArrayRowIdsParts[i]->rowsNum;
    }
    
    InputArray* combinedInputArrayRowIds = new InputArray(totalPartsRowsNum);
    uint64_t combinedInputArrayRowIndex = 0;

    for (int i = 0; i < partsNum - 1; i++) {
        InputArray* part = inputArrayRowIdsParts[i];

        memcpy(combinedInputArrayRowIds->columns[0] + combinedInputArrayRowIndex, part->columns[0], part->rowsNum*sizeof(uint64_t));
        combinedInputArrayRowIndex += part->rowsNum;
        delete part;

        // for (int j = 0; j < equalPartsSize; j++) {
        //     combinedInputArrayRowIds->columns[0][combinedInputArrayRowIndex++] = inputArrayRowIdsParts[i]->columns[0][j];
        // }
    }
    InputArray* lastPart = inputArrayRowIdsParts[partsNum - 1];

    memcpy(combinedInputArrayRowIds->columns[0] + combinedInputArrayRowIndex, lastPart->columns[0], lastPart->rowsNum*sizeof(uint64_t));

    combinedInputArrayRowIndex += lastPart->rowsNum;
    delete lastPart;
    // for (int j = 0; j < lastPartSize; j++) {
    //     combinedInputArrayRowIds->columns[combinedInputArrayRowIndex++] = 
    // }

    combinedInputArrayRowIds->rowsNum = combinedInputArrayRowIndex; // update rowsNum because the other rows are useless

    return combinedInputArrayRowIds;
}

void waitForJobsToFinish(int queryIndex) {
        // std::cout << "MAIN THREAD WILL WAIT"<<std::endl;
    pthread_mutex_lock(&jobsCounterMutexes[queryIndex]);
    while (jobsCounter[queryIndex] > 0){
        // std::cout<<"jobs counter: "<<jobsCounter[queryIndex]<<std::endl;
        pthread_cond_wait(&jobsCounterConds[queryIndex], &jobsCounterMutexes[queryIndex]);
    }

    pthread_mutex_unlock(&jobsCounterMutexes[queryIndex]);
}

InputArray* parallelFilterOrInnerJoin(int queryIndex, InputArray* inputArrayRowIds, bool isFilterJob, int field1Id, int field2Id, int operation, uint64_t numToCompare, const InputArray* pureInputArray) {
    int threadsNum = scheduler->getThreadsNum();
    InputArray* filteredInputArrayRowIdsParts[threadsNum];
    jobsCounter[queryIndex] = threadsNum;
    uint64_t equalPartsSize = inputArrayRowIds->rowsNum/threadsNum;
    uint64_t remainingRowIds = inputArrayRowIds->rowsNum%threadsNum;
    for (int j = 0; j < threadsNum; j++) {
        uint64_t startIndex = j*equalPartsSize;
        uint64_t stopIndex = startIndex + equalPartsSize;
        if (j == threadsNum - 1) {
            stopIndex += remainingRowIds;
        }
        // std::cout << "queryIndex: "<<queryIndex<<", creating filter job with startIndex: "<<startIndex<<" and stopIndex: "<<stopIndex<<std::endl;
        Job* job;
        if (isFilterJob) {
            job = new filterJob(field1Id, operation, numToCompare, pureInputArray, startIndex, stopIndex, inputArrayRowIds, &filteredInputArrayRowIdsParts[j], queryIndex);
        } else {
            job = new innerJoinJob(field1Id, field2Id, pureInputArray, startIndex, stopIndex, inputArrayRowIds, &filteredInputArrayRowIdsParts[j], queryIndex);
        }
        scheduler->schedule(job, -1);
    }

    waitForJobsToFinish(queryIndex);
                    // std::cout << "MAIN THREAD CONTINUED"<<std::endl;

    delete inputArrayRowIds;
                    // std::cout << "MAIN THREAD CONTINUED1"<<std::endl;

    return combineInputArrayRowIds(filteredInputArrayRowIdsParts, threadsNum);
}

inline uint64_t hashFunction(uint64_t payload, int shift) {
    return (payload >> (8 * shift)) & 0xFF;
}

//parallel join by splitting in parts according to their first byte
//same as described in class
result* managejoin_3(relation* R, relation* S, int queryIndex)
{
    uint64_t* hist=histcreate(S->tuples, S->num_tuples, 0);
    uint64_t* psum=psumcreate(hist);
    int parts = scheduler->getThreadsNum();
    int i = 0;
    list** lst=new list*[power];    //if one is empty (probably not) then ???
    jobsCounter[queryIndex]=power;
    for (int j = 0; j < power; j++)
    {
        lst[j] = new list(131072, 2); //probably not correct
        if (j == power - 1)
            scheduler->schedule(new jJob(R->tuples, S->tuples, R->num_tuples, psum[j], S->num_tuples, lst[j], queryIndex), -1); 
        else
            scheduler->schedule(new jJob(R->tuples, S->tuples, R->num_tuples, psum[j], psum[j+1], lst[j], queryIndex), -1); 

    }
    pthread_mutex_lock(&predicateJobsDoneMutexes[queryIndex]);
    while(jobsCounter[queryIndex]>0)
        pthread_cond_wait(&predicateJobsDoneConds[queryIndex],&predicateJobsDoneMutexes[queryIndex]);
    pthread_mutex_unlock(&predicateJobsDoneMutexes[queryIndex]);

    result* rslt=new result;
    rslt->lst=NULL;
    for(int i=0;i<parts;i++)
    {
        if(lst[i]->rows>0)
        {
            if(rslt->lst==NULL)
                rslt->lst=lst[i];
            else
            {
                rslt->lst->last->next=lst[i]->first;
                rslt->lst->last=lst[i]->last;
                rslt->lst->rows+=lst[i]->rows;
                lst[i]->first=lst[i]->last=NULL;
                delete lst[i];
            }
        }
    }
    delete[] lst;
    if(rslt->lst==NULL)
        rslt->lst=new list(1,2);
    return rslt;
}

//parallel join by splitting in parts according to their first byte, 
//but combine some parts together so threads == number of jobs 
result* managejoin_2(relation* R, relation* S, int queryIndex)
{
    uint64_t* hist=histcreate(S->tuples, S->num_tuples, 0);
    uint64_t* psum=psumcreate(hist);
    int parts = scheduler->getThreadsNum();
    int size = power / parts; //size of each join portion
    int i = 0, start = 0, end = size;
    list** lst=new list*[parts];
    jobsCounter[queryIndex]=parts;
    for (int j = 0; j < parts; j++)
    {
        lst[j] = new list(131072, 2); //probably not correct
        if (j == parts - 1)
            scheduler->schedule(new jJob(R->tuples, S->tuples, R->num_tuples, psum[start], S->num_tuples, lst[j], queryIndex), -1); 
        else
            scheduler->schedule(new jJob(R->tuples, S->tuples, R->num_tuples, psum[start], psum[end+1], lst[j], queryIndex), -1); 

        start = end;
        end = end+size;
    }
    pthread_mutex_lock(&predicateJobsDoneMutexes[queryIndex]);
    while(jobsCounter[queryIndex]>0)
        pthread_cond_wait(&predicateJobsDoneConds[queryIndex],&predicateJobsDoneMutexes[queryIndex]);
    pthread_mutex_unlock(&predicateJobsDoneMutexes[queryIndex]);

    result* rslt=new result;
    rslt->lst=NULL;
    for(int i=0;i<parts;i++)
    {
        if(lst[i]->rows>0)
        {
            if(rslt->lst==NULL)
                rslt->lst=lst[i];
            else
            {
                rslt->lst->last->next=lst[i]->first;
                rslt->lst->last=lst[i]->last;
                rslt->lst->rows+=lst[i]->rows;
                lst[i]->first=lst[i]->last=NULL;
                delete lst[i];
            }
        }
    }
    delete[] lst;
    if(rslt->lst==NULL)
        rslt->lst=new list(1,2);
    return rslt;
}

//parallel join by splitting tuples in equal parts
result* managejoin(relation* R, relation* S, int queryIndex)
{
        // JobScheduler * scheduler=new JobScheduler(1,100000);
        // int partsize=1000;
        // int parts=(S->num_tuples/partsize)+1;    
    int parts=scheduler->getThreadsNum();
    int partsize=(S->num_tuples/parts)+1;
    list** lst=new list*[parts];
    // bool* done=new bool[parts]{false};
    jobsCounter[queryIndex]=parts;
    // pthread_mutex_lock(&DoneJoinMutex);
    // std::cout<<"start"<<std::endl;
    // std::cout<<parts<<std::endl;
    for(int i=0;i<parts;i++)
    {
        // std::cout<<i<<" of "<<parts<<std::endl;
        lst[i]=new list(131072,2);
        int start=i*partsize;
        int end=(i+1)*partsize;
        if(end>S->num_tuples)
            end=S->num_tuples;
        bool b;

        scheduler->schedule(new jJob(R->tuples,S->tuples,R->num_tuples,start,end,lst[i],queryIndex),-1); 
        // std::cout<<start<<" "<<end<<" "<<S->num_tuples<<std::endl;
        // joinparallel(R->tuples,S->tuples,R->num_tuples,start,end,lst[i],&b);
    }
    pthread_mutex_lock(&predicateJobsDoneMutexes[queryIndex]);
    while(jobsCounter[queryIndex]>0)
    {
        pthread_cond_wait(&predicateJobsDoneConds[queryIndex],&predicateJobsDoneMutexes[queryIndex]);
    }
    pthread_mutex_unlock(&predicateJobsDoneMutexes[queryIndex]);

    // pthread_mutex_lock(&LastJoinMutex);
    // if(done>0)
    // {
    //     pthread_mutex_unlock(&DoneJoinMutex);
    //     pthread_cond_wait(&LastJoinCond,&LastJoinMutex);
    // }
    // else pthread_mutex_unlock(&DoneJoinMutex);

    // pthread_mutex_unlock(&LastJoinMutex);
    // std::cout<<"waited"<<std::endl;
    result* rslt=new result;
    rslt->lst=NULL;
    // rslt->lst=lst[0];
    for(int i=0;i<parts;i++)
    {
        // std::cout<<i<<std::endl;
        if(lst[i]->rows>0)
        {
            if(rslt->lst==NULL)
                rslt->lst=lst[i];
            else
            {
                rslt->lst->last->next=lst[i]->first;
                rslt->lst->last=lst[i]->last;
                rslt->lst->rows+=lst[i]->rows;
                lst[i]->first=lst[i]->last=NULL;
                delete lst[i];

            }
        }
    }
    delete[] lst;
    // delete scheduler;
    // std::cout<<"cool"<<std::endl;
    if(rslt->lst==NULL)
        rslt->lst=new list(1,2);
    return rslt;
    // int size=131072; //bytes
}

void joinparallel(tuple* R, tuple* S, int Rsize, int Sstart, int Send,list* lst,int queryIndex)
{
    // std::cout<<"join parl "<<std::endl;
    int64_t samestart=-1;
    for(uint64_t r=0,s=Sstart;r<Rsize&&s<Send;)
    {
        int64_t dec=R[r].payload-S[s].payload;
        if(dec==0)
        {
            lst->insert(R[r].key);
            lst->insert(S[s].key);
            switch(samestart)
            {
                case -1:
                    if(s+1<Send&&S[s].payload==S[s+1].payload)
                        samestart=s;
                    break;
                default:
                    if(S[samestart].payload!=S[s].payload)  
                        samestart=-1;
            }
            if(s+1<Send&&S[s].payload==S[s+1].payload)
                s++;      
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
    pthread_mutex_lock(&predicateJobsDoneMutexes[queryIndex]);
    // std::cout<<"i'm done"<<std::endl;
    jobsCounter[queryIndex]--;
    if(jobsCounter[queryIndex]==0)
        pthread_cond_signal(&predicateJobsDoneConds[queryIndex]);
    pthread_mutex_unlock(&predicateJobsDoneMutexes[queryIndex]);
    // if(islast)
    // {
    //     std::cout<<"im last"<<std::endl;
    //     pthread_cond_signal(&LastJoinCond);
    //     std::cout<<"did it"<<std::endl;
    // }

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
                    break;
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
uint64_t cntrcntr;

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
        if(hist[i] > TUPLES_PER_BUCKET && shift < 7){
        // std::cout<<"array:"<<array<<", offset: "<<offset<<std::endl;
            tuplereorder_serial(array+start,array2+start,psum[i]-start,shift+1); //psum[i]-start = endoffset
        }
        else            
            quickSort(array,start, psum[i]-1, -1, -1, false);

        start=psum[i];
    }
    delete[] psum;
    delete[] hist;
}
uint64_t myCounter = 0;
static pthread_mutex_t blah = PTHREAD_MUTEX_INITIALIZER; // mutex for JobQueue
void tuplereorder_parallel(tuple* array,tuple* array2, int offset,int shift, bool isLastCall, int reorderIndex /*can be 0 or 1*/, int queryIndex)
{
            // std::cout<<"reorder started -> queryIndex: "<<queryIndex<<", reorderIndex: "<<reorderIndex<<std::endl;

    // std::cout<<"array:"<<array<<", offset: "<<offset<<", isLastCall: "<<isLastCall<<std::endl;
    uint64_t* hist=histcreate(array,offset,shift);
    uint64_t* psum=psumcreate(hist);
    trJob *reorder = NULL;
    qJob *quick = NULL;

    for(int i=0;i<offset;i++)
    {
        uint64_t hash=hashFunction(array[i].payload,7-shift);
        array2[psum[hash]].key = array[i].key;
        array2[psum[hash]].payload = array[i].payload;
        psum[hash]++;
    }
    memcpy(array,array2,offset*sizeof(tuple));

    int lastReorderCallIndex = -1;
    int lastQuicksortCallIndex = -1;
    // if (reorderIndex == 0)
    //     std::cout<<"1, reorderIndex: "<<reorderIndex<<std::endl;

    if (isLastCall) {
        for(int i=0,start=0;i<power;i++)
        {
            // std::cout<<"reorderIndex: "<<reorderIndex<<" -> "<<i<<" of "<<power<<std::endl;
            if(hist[i]==0)
                continue;
            if(hist[i] > TUPLES_PER_BUCKET && shift < 7)
            {
                // std::cout<<"should break bucket"<<std::endl;
                
                    // lastCallIndex = i;
                
                lastReorderCallIndex = i;
                // isLastCall = false;
            }
            else
            {
                lastQuicksortCallIndex = i;
            }   
            start=psum[i];
        }
    }
    // if (reorderIndex == 0)
        // std::cout<<"1, reorderIndex: "<<reorderIndex<<", isLastCall: "<<isLastCall<<std::endl;
    // if (isLastCall && lastCallIndex == -1)
        // std::cout<<"reorderIndex: "<<reorderIndex<<"between fors"<<std::endl;
    bool allHistsEmpty = true;
    for(int i=0,start=0;i<power;i++)
    {
        // if (reorderIndex == 0)
            // std::cout<<"1, reorderIndex: "<<reorderIndex<<", i: "<<i<<"hist[i]: "<<hist[i]<<std::endl;
        if(hist[i]==0)
            continue;
        allHistsEmpty = false;
        // std::cout<<"shift: "<<shift<<std::endl;
        if (shift == 7){
            myCounter++;
        }
        if(hist[i] > TUPLES_PER_BUCKET && shift < 7)
        {
            
            // std::cout<<"will scheduler new reorder -> queryIndex: "<<queryIndex<<", reorderIndex: "<<reorderIndex<<std::endl;
            if(reorderMode==parallel && ( newJobPerBucket || shift%2 == 0 ))
                scheduler->schedule(new trJob(array+start,array2+start,psum[i]-start,shift+1, i==lastReorderCallIndex ? true : false, reorderIndex, queryIndex), queryIndex);
            else
                tuplereorder_parallel(array+start,array2+start,psum[i]-start,shift+1, i==lastReorderCallIndex ? true : false, reorderIndex, queryIndex);
                // tuplereorder_serial(array+start,array2+start,psum[i]-start,shift+1);
            // delete reorder;
        } else {
            bool isLastQuickSort = (lastQuicksortCallIndex == i && lastReorderCallIndex == -1);
            // std::cout<<isLastQuickSort<<" "<<lastQuicksortCallIndex<<std::endl;
            // std::cout<<"queryIndex = "<<queryIndex<<", reorderIndex: "<<reorderIndex<<", isLastQuickSort: "<<isLastQuickSort<<", lastQuicksortCallIndex: "<<lastQuicksortCallIndex<<", lastReorderCallIndex"<<lastReorderCallIndex<<std::endl;
            if (quickSortMode == serial)
                quickSort(array,start, psum[i]-1, queryIndex, reorderIndex, isLastQuickSort);
            else if (quickSortMode == parallel)
            {
                scheduler->schedule(new qJob(array,start, psum[i]-1, queryIndex, reorderIndex, isLastQuickSort), queryIndex);
                //delete quick;
            }
        }
        start=psum[i];

    }
    // std::cout<<"isLastCall: "<<isLastCall<<", lastCallIndex: "<<lastCallIndex<<std::endl;
    
    // if (reorderMode == parallel &&  isLastCall && lastCallIndex == -1) {
    //     // std::cout<<"In if -> isLastCall: "<<isLastCall<<", lastCallIndex: "<<lastCallIndex<<", reorderIndex: "<<reorderIndex<<std::endl;
    //     // scheduler->~JobScheduler();
    //     // scheduler = NULL;
    //     // std::cout<<"queryIndex: "<<queryIndex<<", reorderIndex: "<<reorderIndex<<std::endl;
        
    //     lastJobDoneArrays[queryIndex][reorderIndex] = true;
    //     pthread_cond_signal(&predicateJobsDoneConds[queryIndex]);
    // }
    if (allHistsEmpty) {
         lastJobDoneArrays[queryIndex][reorderIndex] = true;
         pthread_cond_signal(&predicateJobsDoneConds[queryIndex]);
    }
    delete[] psum;
    delete[] hist;
}

void tuplereorder(tuple* array,tuple* array2, int offset,int shift, int reorderIndex, int queryIndex)
{
    // if (reorderMode == serial)
    //     tuplereorder_serial(array, array2, offset, shift);
    // else if (reorderMode == parallel)
    // {
    //     // if (scheduler == NULL)
    //     //     scheduler = new JobScheduler(32, 1000);
    // }
            tuplereorder_parallel(array, array2, offset, shift, true, reorderIndex, queryIndex);

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
    unsigned int seed = stopIndex - startIndex;
    return rand_r(&seed)%(stopIndex - startIndex + 1) + startIndex;
}

int partition(tuple* tuples, int startIndex, int stopIndex)
{ 
    // pthread_mutex_lock(&blah);
    // cntrcntr++;
    // pthread_mutex_unlock(&blah);
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

void quickSort(tuple* tuples, int startIndex, int stopIndex, int queryIndex, int reorderIndex, bool isLastCall)
{
    // pthread_mutex_lock(&blah);
    // cntr++;
    // std::cout<<" quicksort "<<cntr<<" start: "<<startIndex<<" stopIndex: "<<std::endl;
    // pthread_mutex_unlock(&blah);

    
    if (startIndex < stopIndex) 
    { 
        int partitionIndex = partition(tuples, startIndex, stopIndex); 
  
        quickSort(tuples, startIndex, partitionIndex - 1, queryIndex, reorderIndex, false); 
        quickSort(tuples, partitionIndex + 1, stopIndex, queryIndex, reorderIndex, false); 
    } 

    //(quickSortMode == parallel || reorderMode == parallel) && 
    if (isLastCall) {
        // std::cout<<"islastcall qs"<<std::endl;
        // std::cout<<"queryIndex: "<<queryIndex<<" last call, "<<"reorderIndex: "<<reorderIndex<<std::endl;
        lastJobDoneArrays[queryIndex][reorderIndex] = true;
        pthread_cond_signal(&predicateJobsDoneConds[queryIndex]);
    }
}

// // (startIndex, stopIndex) -> inclusive
// void sortBucket(relation* rel, int startIndex, int stopIndex) {
//     // stopIndex--;
//     //std::cout<<"sort "<<startIndex<<" "<<stopIndex<<std::endl;
//     quickSort(rel->tuples, startIndex, stopIndex);
// }

InputArray** readArrays() {
    InputArray** inputArrays = new InputArray*[MAX_INPUT_ARRAYS_NUM]; // size is fixed
    for (int i = 0; i < MAX_INPUT_ARRAYS_NUM; i++) {
        inputArrays[i] = NULL;
    }
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

        InputArray* curInputArray = new InputArray(rowsNum, columnsNum);
        curInputArray->initStatistics();
        ColumnStats* curColumnsStats = curInputArray->columnsStats;

        for (uint64_t i = 0; i < columnsNum; i++) {
            ColumnStats* curStats = &curColumnsStats[i];
            curStats->valuesNum = rowsNum;
            curStats->maxValue = 0;
            curStats->minValue = UINT64_MAX;

            for (uint64_t j = 0; j < rowsNum; j++) {
                if ((rtn = fread(&curInputArray->columns[i][j], sizeof(uint64_t), 1, fileP)) < 0)
                {
                    printf("fread for file <%s> returned %ld\n", fileName, rtn);
                    return NULL;
                }
                
                uint64_t curValue = curInputArray->columns[i][j];
                if (curValue < curStats->minValue) {
                    curStats->minValue = curValue;
                }
                if (curValue > curStats->maxValue) {
                    curStats->maxValue = curValue;
                }
            }

            curStats->calculateDistinctValuesNum(curInputArray, i);
            // uint64_t boolArraySize = curStats->maxValue - curStats->minValue + 1;
            // bool arraySizeCut = false;
            // if (boolArraySize > MAX_BOOLEAN_ARRAY_SIZE) {
            //     boolArraySize = MAX_BOOLEAN_ARRAY_SIZE;
            //     arraySizeCut = true;
            // }
            // curStats->distinctValuesNum = curStats->valuesNum;
            // bool* boolArray=new bool[boolArraySize]{false};
            // for (uint64_t j = 0; j < rowsNum; j++) {
            //     uint64_t boolArrayIndex = curInputArray->columns[i][j] - curStats->minValue;
            //     if (arraySizeCut) {
            //         boolArrayIndex %= MAX_BOOLEAN_ARRAY_SIZE;
            //     }

            //     if (!boolArray[boolArrayIndex]) {
            //         boolArray[boolArrayIndex] = true;
            //     } else {
            //         curStats->distinctValuesNum--;
            //     }
            // }
            // delete[] boolArray;
        }

        fclose(fileP);
        
        inputArrays[inputArraysIndex] = curInputArray;

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

void handlequery(char** parts,const InputArray** allrelations, int queryIndex)
{
    myCounter = 0;
    // std::cout<<"query: "<<queryIndex<<std::endl;
    // std::cout<<"thread "<<pthread_self()<<", inputArrays = "<<allrelations<<", inputArrays address = "<<&allrelations<<std::endl;
    /*for(int i=0;i<3;i++)
    {
        std::cout<<parts[i]<<std::endl;
    }*/
    int relationIds[MAX_INPUT_ARRAYS_NUM];
    int relationsnum;
    // std::cout<<"1, queryIndex: "<<queryIndex<<std::endl;
    // pthread_mutex_lock(&blah);
    loadrelationIds(relationIds, parts[0], relationsnum);
    // pthread_mutex_unlock(&blah);

        // std::cout<<"2, queryIndex: "<<queryIndex<<std::endl;

    IntermediateArray* result=handlepredicates(allrelations,parts[1],relationsnum, relationIds, queryIndex);
        // std::cout<<"3, queryIndex: "<<queryIndex<<std::endl;

    // handleprojection(result,allrelations,parts[2], relationIds,queryIndex);
    manageprojection(result,allrelations,parts[2], relationIds,queryIndex);

            // std::cout<<"4, queryIndex: "<<queryIndex<<std::endl;

    if(result!=NULL)
        delete result;
    // delete[] parts;   

    if (queryMode == parallel) {
        // std::cout<<std::endl;
        pthread_mutex_lock(&queryJobDoneMutex);
        queryJobDone--;
        available_threads++;
        // if(queryJobDone==0)
            pthread_cond_signal(&queryJobDoneCond);
        pthread_mutex_unlock(&queryJobDoneMutex);

                // std::cout<<"query "<<queryIndex<<" ended"<<std::endl;
    }
    // std::cout<<"mycounter: "<<myCounter<<std::endl;
}
void loadrelationIds(int* relationIds, char* part, int& relationsnum)
{
    // std::cout<<"LOADRELATIONS: "<<part<<std::endl;
    // pthread_mutex_lock(&blah);
    int cntr=1, start = 0;
    for(int i=0;part[i]!='\0';i++)
    {
        if(part[i]==' ')
        {
            part[i] = '\0';
            relationIds[cntr - 1] = atoi(part + start);
            start = i + 1;
            cntr++;
        }
    }
    relationIds[cntr - 1] = atoi(part + start);
    relationsnum=cntr;
    // pthread_mutex_unlock(&blah);

    // backup
    // // char tempPart[strlen(part) + 1];
    // std::cout<<"part: "<<part<<std::endl;
    // char * tempPart = new char[strlen(part) + 1];
    // mempcpy(tempPart, part, strlen(part) + 1);
    // char *buffer;

    // // char *buffer = new
    // // strcpy(tempPart, part);
    // int i = 0;
    // char* token = strtok_r(tempPart, " ", &buffer);
    // while (token) {
    //     relationIds[i++] = atoi(token);
    //     // cntr++;
    //     std::cout<<"token: "<<token<<std::endl;
    //     token = strtok_r(NULL, " ", &buffer);
    // }
    // std::cout<<"part: "<<part<<", relationsNum = "<<i<<std::endl;
    // relationsnum=i;
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

bool handleReorderRel(relation* rel, tuple** t, uint64_t** preds, int cntr, int i, int predicateArrayId, int fieldId, bool prevPredicateWasFilterOrSelfJoin, int queryIndex, int reorderIndex) {
    bool shouldSortRel = shouldSort(preds, cntr, i, predicateArrayId, fieldId, prevPredicateWasFilterOrSelfJoin);
    // tuple* t = NULL;
    // std::cout<<"num_tuples: "<<rel->num_tuples<<std::endl;
    // std::cout<<"shouldSortRel1: "<<shouldSortRel1<<std::endl;
    if (shouldSortRel) {
        (*t)=new tuple[rel->num_tuples];
        tuplereorder(rel->tuples,*t,rel->num_tuples,0, reorderIndex, queryIndex);  //add parallel
        //mid_func(rel1.tuples,t,rel1.num_tuples,0);

        // delete[] t1;
    }

    return shouldSortRel;
}

void waitForReorderJobsToBeQueued(int queryIndex, int reorderIndex) {
    pthread_mutex_lock(&predicateJobsDoneMutexes[queryIndex]);
    while (lastJobDoneArrays[queryIndex][reorderIndex] == false){
        pthread_cond_wait(&predicateJobsDoneConds[queryIndex], &predicateJobsDoneMutexes[queryIndex]);
    }
        // std::cout<<"-------------MAIN THREAD1"<<std::endl;

    pthread_mutex_unlock(&predicateJobsDoneMutexes[queryIndex]);
    lastJobDoneArrays[queryIndex][reorderIndex] = false;
}

void handleDelete(uint64_t** preds, int cntr, int relationsnum, InputArray** inputArraysRowIds, result* rslt, bool onlyResult) {
    if (rslt != NULL) {
        delete rslt->lst;
        delete rslt;
    }

    if (onlyResult)
        return;

    for(int i=0;i<cntr;i++)
    {
        delete[] preds[i];
    }
    delete[] preds;
    for(int i=0;i<relationsnum;i++)
        delete inputArraysRowIds[i];
    delete[] inputArraysRowIds;
}
// int xxx=0;
IntermediateArray* handlepredicates(const InputArray** inputArrays,char* part,int relationsnum, int* relationIds, int queryIndex)
{
    int cntr;
    uint64_t** preds=splitpreds(part,cntr);
    // preds=optimizepredicates(preds,cntr,relationsnum,relationIds);
    // xxx++;
    // if(xxx==1)
    // {
    // std::cout<<"flag is: "<<OptimizePredicatesFlag<<std::endl;
    if(OptimizePredicatesFlag)
        preds = OptimizePredicates(preds,cntr,relationsnum,relationIds,inputArrays);
    else preds=optimizepredicates(preds,cntr,relationsnum,relationIds);
        // exit(1);
    // }
    // std::cout<<"after optimization"<<std::endl;
    // for(int i=0;i<cntr;i++)
    // {
    //     for(int j=0;j<5;j++)
    //     {
    //         std::cout<<preds[i][j]<<" ";
    //     }
    //     std::cout<<std::endl;
    // }
    InputArray** inputArraysRowIds = new InputArray*[relationsnum];
    for (int i = 0; i < relationsnum; i++) {
        // std::cout<<"thread "<<pthread_self()<<", relationIds[i] = "<<relationIds[i]<<" ====================================================================="<<std::endl;
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
        
        const InputArray* inputArray1 = inputArrays[inputArray1Id];
        const InputArray* inputArray2 = isFilter ? NULL : inputArrays[inputArray2Id];
        InputArray* inputArray1RowIds = inputArraysRowIds[predicateArray1Id];
        InputArray* inputArray2RowIds = isFilter ? NULL : inputArraysRowIds[predicateArray2Id];

        uint64_t field1Id = preds[i][1];
        uint64_t field2Id = preds[i][4];
        int operation = preds[i][2];
        if (isFilter) {
            uint64_t numToCompare = field2Id;
            InputArray* filteredInputArrayRowIds;
            if (filterMode == serial) {
                filteredInputArrayRowIds = inputArray1RowIds->filterRowIds(field1Id, operation, numToCompare, inputArray1, 0, inputArray1RowIds->rowsNum);
                delete inputArray1RowIds;
            } else if (filterMode == parallel) {
                filteredInputArrayRowIds = parallelFilterOrInnerJoin(queryIndex, inputArray1RowIds, true, field1Id, 0, operation, numToCompare, inputArray1);
            }
            inputArraysRowIds[predicateArray1Id] = filteredInputArrayRowIds;
            prevPredicateWasFilterOrSelfJoin = true;
            continue;
        }

        // std::cout<<"predicateArray1Id: "<<predicateArray1Id<<", predicateArray2Id: "<<predicateArray2Id<<std::endl;

        // switch (operation)
        // {
        // case 2: // '='

        // Below we are handling only the '=' operation
        if (predicateArray1Id == predicateArray2Id) {
            // self-join of InputArray
            InputArray* filteredInputArrayRowIds;
            if (filterMode == serial) {
                filteredInputArrayRowIds = inputArray1RowIds->filterRowIds(field1Id, field2Id, inputArray1, 0, inputArray1RowIds->rowsNum);
                delete inputArray1RowIds;
            } else if (filterMode == parallel) {
                filteredInputArrayRowIds = parallelFilterOrInnerJoin(queryIndex, inputArray1RowIds, false, field1Id, field2Id, -1, 0, inputArray1);
            }

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

        // {
        relation rel1, rel2;
        bool rel2ExistsInIntermediateArray = false;

        // std::cout<<"predicateArray1Id: "<<predicateArray1Id<<", field1Id: "<<field1Id<<", predicateArray2Id: "<<predicateArray2Id<<", field2Id: "<<field2Id<<std::endl;

        // fill rel1
        if (curIntermediateArray == NULL || !curIntermediateArray->hasPredicateArrayId(predicateArray1Id)) {
                                    // std::cout<<"predicateArray1Id does NOT exist in intermediate array"<<std::endl;

            inputArray1RowIds->extractColumnFromRowIds(rel1, field1Id, inputArray1);
        } else {
                        // std::cout<<"predicateArray1Id exists in intermediate array"<<std::endl;


            curIntermediateArray->extractFieldToRelation(&rel1, inputArray1, predicateArray1Id, field1Id);
        }

        // fill rel2
        if (curIntermediateArray == NULL || !curIntermediateArray->hasPredicateArrayId(predicateArray2Id)) {
                                    // std::cout<<"predicateArray2Id does NOT exist in intermediate array"<<std::endl;

            inputArray2RowIds->extractColumnFromRowIds(rel2, field2Id, inputArray2);
        } else {
                                    // std::cout<<"predicateArray2Id exists in intermediate array"<<std::endl;


            rel2ExistsInIntermediateArray = true;
            curIntermediateArray->extractFieldToRelation(&rel2, inputArray2, predicateArray2Id, field2Id);
        }

        // relation* newRel1 = new relation();
        // relation* reorderedRel1=&rel1;
        // std::cout<<"rel1 size: "<<reorderedRel1->num_tuples<<std::endl;
        tuple* t1 = NULL;
        bool shouldSortRel1 = handleReorderRel(&rel1, &t1, preds, cntr, i, predicateArray1Id, field1Id, prevPredicateWasFilterOrSelfJoin, queryIndex, 0);
        // tuple* t1 = NULL;
        // bool shouldSortRel1 = shouldSort(preds, cntr, i, predicateArray1Id, field1Id, prevPredicateWasFilterOrSelfJoin);
        // // std::cout<<"shouldSortRel1: "<<shouldSortRel1<<std::endl;
        // if (shouldSortRel1) {
        //     t1=new tuple[rel1.num_tuples];
        //     tuplereorder(rel1.tuples,t1,rel1.num_tuples,0, 0, queryIndex);  //add parallel
        //     //mid_func(rel1.tuples,t,rel1.num_tuples,0);

        //     // delete[] t1;
        // }

        
        
        // relation* newRel2 = new relation();
        // relation* reorderedRel2=&rel2;
        // std::cout<<"rel2 size: "<<reorderedRel2->num_tuples<<std::endl;
        tuple* t2 = NULL;
        bool shouldSortRel2 = handleReorderRel(&rel2, &t2, preds, cntr, i, predicateArray2Id, field2Id, prevPredicateWasFilterOrSelfJoin, queryIndex, 1);
        // std::cout<<"shouldSortRel1: "<<shouldSortRel1<<", shouldSortRel2: "<<shouldSortRel2<<std::endl;
        // tuple* t2 = NULL;
        // bool shouldSortRel2 = shouldSort(preds, cntr, i, predicateArray2Id, field2Id, prevPredicateWasFilterOrSelfJoin);
        //                     // std::cout<<"shouldSortRel2: "<<shouldSortRel2<<std::endl;
        // if (shouldSortRel2) {
        //     t2=new tuple[rel2.num_tuples];
        //     tuplereorder(rel2.tuples,t2,rel2.num_tuples,0, 1, queryIndex);  //add parallel
        //     //mid_func(rel2.tuples,t,rel2.num_tuples,0);
        //     // delete[] t2;
        // }
                //  std::cout<<"-------------MAIN THREAD3"<<std::endl;

        if (reorderMode == parallel || quickSortMode==parallel) {
            // std::cout<<"query "<<queryIndex<<" is waiting for predicate jobs to finish... shouldSortRel1 = "<<shouldSortRel1<<", shouldSortRel2 = "<<shouldSortRel2<<std::endl;
            if (shouldSortRel1) {
                waitForReorderJobsToBeQueued(queryIndex, 0);
            }
            // std::cout<<"middle"<<std::endl;
            if (shouldSortRel2){
                waitForReorderJobsToBeQueued(queryIndex, 1);
            }
            // sleep(1);
            // pthread_mutex_lock(&predicateJobsDoneMutexes[queryIndex]);
            waitForJobsToFinish(queryIndex);
            // while(1)
            //     std::cout<<"jobcntr "<<jobsCounter[queryIndex]<<" "<<queryIndex<<std::endl;

            // pthread_mutex_unlock(&predicateJobsDoneMutexes[queryIndex]);

            // std::cout<<"query "<<queryIndex<<" stopped waiting for predicate jobs to finish"<<std::endl;

            // scheduler->~JobScheduler();
            // scheduler = NULL;

        }
        
        // std::cout<<cntrcntr<<std::endl;
        // rel1.print();
        
        // rel2.print();
        // std::cout<<"-------------MAIN THREAD5"<<std::endl;
        if (t1 != NULL)
            delete[] t1;
        if (t2 != NULL)
            delete[] t2;
        result* rslt;
        if (joinMode == serial) {
            // rel2ExistsInIntermediateArray = false;
            rslt= join(rel2ExistsInIntermediateArray ? &rel2 : &rel1, rel2ExistsInIntermediateArray ? &rel1 : &rel2, inputArray1->columns, inputArray2->columns, inputArray1->columnsNum, inputArray2->columnsNum, 0);
        } else if (joinMode == parallel) {
            rslt = managejoin(rel2ExistsInIntermediateArray ? &rel2 : &rel1, rel2ExistsInIntermediateArray ? &rel1 : &rel2,queryIndex);
        }
        
        if (rslt->lst->rows == 0) {
            // no results
            handleDelete(preds, cntr, relationsnum, inputArraysRowIds, rslt, false);
            return NULL;
        }
        // rslt->lst->print();
        // std::cout<<rslt->lst->rows<<std::endl;
        uint64_t** resultArray=rslt->lst->lsttoarr();
        // for (uint64_t i = 0; i < rslt->lst->rows; i++) {
        //     for (uint64_t j = 0; j < rslt->lst->rowsz; j++) {
        //         std::cout<<resultArray[j][i]<<" ";
        //     }
        //     std::cout<<std::endl;
        // }
        uint64_t rows=rslt->lst->rows;
        uint64_t rowsz=rslt->lst->rowsz;
        handleDelete(preds, cntr, relationsnum, inputArraysRowIds, rslt, true);

        // delete newRel1;
        // delete newRel2;                    
        
        if (curIntermediateArray == NULL) {
            // first join
            curIntermediateArray = new IntermediateArray(2);
            curIntermediateArray->populate(resultArray, rows, NULL, inputArray1Id, inputArray2Id, predicateArray1Id, predicateArray2Id);
        } else {
            IntermediateArray* newIntermediateArray = new IntermediateArray(curIntermediateArray->columnsNum + 1);
            newIntermediateArray->populate(resultArray, rows, curIntermediateArray, -1, rel2ExistsInIntermediateArray ? inputArray1Id : inputArray2Id, -1, rel2ExistsInIntermediateArray ? predicateArray1Id : predicateArray2Id);
            delete curIntermediateArray;
            // std::cout<<"interm array size: "<<newIntermediateArray->rowsNum<<std::endl;
            // newIntermediateArray->print();
            curIntermediateArray = newIntermediateArray;
        }
        prevPredicateWasFilterOrSelfJoin = false;

        for(int i=0;i<rowsz;i++)
            delete[] resultArray[i];
        delete[] resultArray;
        //         }
        //     break;
        // default:
        //     break;
        // }
        
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

    handleDelete(preds, cntr, relationsnum, inputArraysRowIds, NULL, false);

    return curIntermediateArray != NULL && curIntermediateArray->rowsNum > 0 ? curIntermediateArray : NULL;
}
void handleprojection(IntermediateArray* rowarr,const InputArray** array,char* part, int* relationIds,int queryIndex)
{
    // std::cout<<"HANDLEPROJECTION: "<<part<<std::endl;
    QueryResult[queryIndex][0]='\0';
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
                // key=rowarr->predicateArrayIds[predicatearray];
                key=rowarr->findColumnIndexByPredicateArrayId(predicatearray);
                for(uint64_t i =0;i<rowarr->rowsNum;i++)
                {
                    sum+=array[projarray]->columns[projcolumn][rowarr->results[key][i]];
                }
            }
            if(sum!=0)
                sprintf(QueryResult[queryIndex],"%s%" PRIu64,QueryResult[queryIndex],sum);
            else
                sprintf(QueryResult[queryIndex],"%sNULL",QueryResult[queryIndex]);
            if(part[i]!='\0')
                sprintf(QueryResult[queryIndex],"%s ",QueryResult[queryIndex]);
            //std::cout<<projarray<<"."<<projcolumn<<": ";
            // if(sum!=0)
            //     std::cout<<sum;
            // else
            //     std::cout<<"NULL";
            // if(part[i]!='\0')
            //     std::cout<<" ";
        }
    }
}
void manageprojection(IntermediateArray* rowarr,const InputArray** array,char* part, int* relationIds,int queryIndex)
{
    QueryResult[queryIndex][0]='\0';
    int projarray,projcolumn,predicatearray;
    int projectionscount=0;
    char** buffer;
    if(projectionMode==parallel)
    {
        for(int i=0;part[i]!='\0';i++)
        {
            if(part[i]=='.')
                projectionscount++;
        }
        
        buffer=new char*[projectionscount];
        jobsCounter[queryIndex]=projectionscount;
    }

    for(int i=0,start=0,projid=0;(i==0)||(i>0&&part[i-1])!='\0';i++)
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
            
            if(projectionMode==parallel)
            {
                buffer[projid]=new char[100];
                buffer[projid][0]='\0';
                scheduler->schedule(new pJob(rowarr,array,projarray,predicatearray,projcolumn,buffer[projid],queryIndex),-1);
                projid++;
            }
            else
            {
                uint64_t sum=0;
                if(rowarr!=NULL)
                {
                    uint64_t key;
                    // key=rowarr->predicateArrayIds[predicatearray];
                    key=rowarr->findColumnIndexByPredicateArrayId(predicatearray);
                    for(uint64_t i =0;i<rowarr->rowsNum;i++)
                    {
                        sum+=array[projarray]->columns[projcolumn][rowarr->results[key][i]];
                    }
                }
                if(sum!=0)
                    sprintf(QueryResult[queryIndex],"%s%" PRIu64,QueryResult[queryIndex],sum);
                else
                    sprintf(QueryResult[queryIndex],"%sNULL",QueryResult[queryIndex]);
                if(part[i]!='\0')
                    sprintf(QueryResult[queryIndex],"%s ",QueryResult[queryIndex]);
            }
            // handleprojectionparallel(rowarr,array,projarray,predicatearray,projcolumn,buffer[i]);
            //std::cout<<projarray<<"."<<projcolumn<<": ";
            // if(sum!=0)
            //     std::cout<<sum;
            // else
            //     std::cout<<"NULL";
            // if(part[i]!='\0')
            //     std::cout<<" ";
        }
    }
    if(projectionMode==parallel)
    {
        pthread_mutex_lock(&predicateJobsDoneMutexes[queryIndex]);
        while(jobsCounter[queryIndex]>0)
        {
            pthread_cond_wait(&predicateJobsDoneConds[queryIndex],&predicateJobsDoneMutexes[queryIndex]);
        }
        pthread_mutex_unlock(&predicateJobsDoneMutexes[queryIndex]);
        sprintf(QueryResult[queryIndex],"%s",buffer[0]);
        for(int i=1;i<projectionscount;i++)
        {
            sprintf(QueryResult[queryIndex],"%s %s",QueryResult[queryIndex],buffer[i]);
            delete[] buffer[i];
        }
        delete[] buffer;
    }
    
}
void handleprojectionparallel(IntermediateArray* rowarr,const InputArray** array,int projarray,int predicatearray,int projcolumn,char* buffer,int queryIndex)
{
    uint64_t sum=0;
    if(rowarr!=NULL)
    {
        uint64_t key;
        // key=rowarr->predicateArrayIds[predicatearray];
        key=rowarr->findColumnIndexByPredicateArrayId(predicatearray);
        for(uint64_t i =0;i<rowarr->rowsNum;i++)
        {
            sum+=array[projarray]->columns[projcolumn][rowarr->results[key][i]];
        }
    }
    if(sum!=0)
        sprintf(buffer,"%" PRIu64 ,sum);
    else
        sprintf(buffer,"NULL");

    pthread_mutex_lock(&predicateJobsDoneMutexes[queryIndex]);
    // std::cout<<"i'm done"<<std::endl;
    jobsCounter[queryIndex]--;

    if(jobsCounter[queryIndex]==0)
        pthread_cond_signal(&predicateJobsDoneConds[queryIndex]);
    pthread_mutex_unlock(&predicateJobsDoneMutexes[queryIndex]);
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

void usage(char** argv)
{
    std::cout<<"Query Optimziation Program with thread support"<<std::endl;
    std::cout<<"Usage: "<<argv[0]<<" [OPTIONS] "<<std::endl;
    std::cout<<"   -qr           (QueRy) Run in queries of every batch in parallel"<<std::endl;
    std::cout<<"   -ro           (ReOrder) Run bucket reorder (radix-sort) in parallel"<<std::endl;
    std::cout<<"   -pb           (Reorder -> New job Per Bucket) (\"-ro\" should be provided) Create a new parallel job for each new bucket"<<std::endl;
    std::cout<<"   -qs           (QuickSort)Run quicksorts independently"<<std::endl;
    std::cout<<"   -jn           (JoiN) Runs joins in parallel (split arrays)"<<std::endl;
    std::cout<<"   -pj           (ProJection) Runs projection checksums in parallel"<<std::endl;
    std::cout<<"   -ft           (FilTer) Runs filters in parallel"<<std::endl;
    std::cout<<"   -all          (ALL) Everything runs in parallel"<<std::endl;  
    std::cout<<"   -n <threads>  Specify number of threads to run"<<std::endl;  
    std::cout<<"   -h            Displays this help message"<<std::endl<<std::endl;
    std::cout<<"Default is: everything runs serial"<<std::endl<<std::endl;
    std::cout<<"*Note that <threads> must be greater than the max batch size so that the threads do not hang*"<<std::endl;
    std::cout<<"*Ignoring all invalid arguments*"<<std::endl;
    return;
}
void params(char** argv,int argc)
{
    bool threadsNumGiven = false;
    for(int i=1;i<argc;i++)
    {
        if(strcmp(argv[i],"-h")==0)
        {
            usage(argv);
            exit(1);
        }
        if(strcmp(argv[i],"-qr")==0)
            queryMode=parallel;
        else if(strcmp(argv[i],"-ro")==0)
            reorderMode=parallel;
        else if(strcmp(argv[i],"-qs")==0)
            quickSortMode=parallel;
        else if(strcmp(argv[i],"-jn")==0)
            joinMode=parallel;
        else if(strcmp(argv[i],"-pj")==0)
            projectionMode=parallel;
        else if(strcmp(argv[i],"-ft")==0)
            filterMode=parallel;
        else if(strcmp(argv[i],"-pb")==0)
            newJobPerBucket=true;
        else if(strcmp(argv[i],"-all")==0)
        {
            // std::cout<<"here"<<std::endl;
            queryMode=parallel;
            reorderMode=parallel;
            joinMode=parallel;
            quickSortMode=parallel;
            projectionMode=parallel;
            filterMode=parallel;
        }
        else if(strcmp(argv[i],"-n")==0) {
            if (i + 1 >= argc) {
                std::cout<<"Wrong arguments"<<std::endl;
                exit(1);
            }
            int threadsNum = atoi(argv[i+1]);
            if (threadsNum <= 0) {
                std::cout<<"Wrong arguments"<<std::endl;
                exit(1);
            }
            scheduler=new JobScheduler(threadsNum,1000000000);
            threadsNumGiven = true;
        }
        else if (strcmp(argv[i],"-optimize")==0)
        {
            OptimizePredicatesFlag=true;
        }
    }
    if (!threadsNumGiven && (queryMode == parallel || reorderMode == parallel || quickSortMode == parallel || joinMode == parallel || filterMode == parallel)) {
        std::cout<<"Wrong arguments. Please rerun the program with -n <threads> to specify the number of threads to run"<<std::endl;
        exit(1);
    }
}