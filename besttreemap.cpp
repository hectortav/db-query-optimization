#include "besttreemap.h"

Statistics::Statistics(int min,int max,int numdiscrete,int size)
{
    this->min=min;
    this->max=max;
    this->numdiscrete=numdiscrete;
    this->size=size;
}
Statistics::~Statistics()
{

}

void Predicate::init(int predicateArray1Id, uint64_t field1Id, int predicateArray2Id, uint64_t field2Id) {
    this->predicateArray1Id = predicateArray1Id;
    this->predicateArray2Id = predicateArray2Id;
    this->field1Id = field1Id;
    this->field2Id = field2Id;
}

bool Predicate::operator ==(Predicate &predicate) {
    return predicateArray1Id == predicate.predicateArray1Id && field1Id == predicate.field1Id &&
        predicateArray2Id == predicate.predicateArray2Id && field2Id == predicate.field2Id;
}

bool Predicate::hasCommonArray(Predicate &predicate) {
    return predicateArray1Id == predicate.predicateArray1Id || predicateArray1Id == predicate.predicateArray2Id ||
        predicateArray2Id == predicate.predicateArray1Id || predicateArray2Id == predicate.predicateArray2Id;
}

void Predicate::print(bool printEndl) {
    std::cout<<predicateArray1Id<<"."<<field1Id<<"<op>"<<predicateArray2Id<<"."<<field2Id;
    if (printEndl) {
        std::cout<<std::endl;
    }
}

PredicateArray::PredicateArray() {
    this->size = 0;
    this->array = NULL;
}

PredicateArray::PredicateArray(int size)
{
    this->size = size;
    this->array = new Predicate[size];
}

PredicateArray::PredicateArray(int size, uint64_t** joinPreds) {
    this->size = size;
    array = new Predicate[size];
    for (int i = 0; i < size; i++) {
        uint64_t* curPred = joinPreds[i];
        array[i].init(curPred[0], curPred[1], curPred[3], curPred[4]);
    }
}

PredicateArray::~PredicateArray()
{
    delete[] array;
    array = NULL;
}

void PredicateArray::init(PredicateArray* predicateArray, int size) {
    // std::cout<<"b size: "<<size<<std::endl;
    this->array = new Predicate[size];
        // std::cout<<"e"<<std::endl;

    if (predicateArray != NULL)
        memcpy(this->array, predicateArray->array, sizeof(Predicate)*size);
    this->size = size;
}

bool PredicateArray::contains(Predicate predicate) {
    for (int i = 0; i < size; i++) {
        // if (array[i].field1Id == predicate.field1Id && array[i].predicateArray1Id == predicate.predicateArray1Id &&
        //     array[i].field2Id == predicate.field2Id && array[i].predicateArray2Id == predicate.predicateArray2Id)
        if (array[i] == predicate)
            return true;
    }
    return false;
}

bool PredicateArray::isConnectedWith(Predicate& predicate) {
    for (int i = 0; i < size; i++) {
        if (array[i].hasCommonArray(predicate))
            return true;
    }
    return false;
}

void PredicateArray::populate(PredicateArray *newPredicateArray) {
    memcpy(array, newPredicateArray->array, sizeof(Predicate)*newPredicateArray->size);
}

void PredicateArray::print() {
    for (int i = 0; i < size; i++) {
        array[i].print(false);
        std::cout<<" ";
    }
    std::cout<<std::endl;
}
bool PredicateArray::operator ==(PredicateArray& array)
{
    // std::cout<<"aa"<<std::endl;
    if(this->size!=array.size)
        return 0;

    for(int i=0;i<this->size;i++)
    {
        int found=0;
        for(int j=0;j<array.size;j++)
        {
            // this->array[i].print(true);
            // array.array[j].print(true);
            if(this->array[i]==array.array[j])
            {
                // std::cout<<"found"<<std::endl;
                found=1;
                break;
            }
        }
        if(found==0)
            return 0;
    }
    
    // for(int i=0;i<array.size;i++)
    // {
    //     int found=0;
    //     for(int j=0;j<this->size;j++)
    //     {
    //         if(this->array[j]==array.array[i])
    //         {
    //             found=1;
    //             break;
    //         }
    //     }
    //     if(found==0)
    //         return 0;
    // }
    //MIGHT NEED UNCOMMENTING! WILL SEE
    return 1;
}

uint64_t** PredicateArray::toUintArray() {
    uint64_t** preds = new uint64_t*[size];
    for (int i = 0; i < size; i++) {
        preds[i] = new uint64_t[5];
        preds[i][0] = array[i].predicateArray1Id;
        preds[i][1] = array[i].field1Id;
        preds[i][2] = 2; // '='
        preds[i][3] = array[i].predicateArray2Id;
        preds[i][4] = array[i].field2Id;
    }

    return preds;
}

Key::Key(int sz)
{
    this->KeyArray=new PredicateArray(sz);
    // this->KeyArray->array=arr;
    // this->KeyArray->size=sz;
}

Key::~Key()
{
    delete[] this->KeyArray;
}

Value::Value(int sz)
{
    this->ValueArray=new PredicateArray(sz);
    // this->ValueArray->array=arr;
    // this->ValueArray->size=sz;
    this->stats=NULL;
}
Value::~Value()
{
    delete this->ValueArray;
    if(stats!=NULL)
        delete stats;
}

int factorial(int x)
{
    int sum=1;
    for(int i=x;i>0;i--)
    {
        sum*=i;
    }
    return sum;
}

Map::Map(int queryArraysNum)
{
    values=new Value*[factorial(queryArraysNum)];
    keys=new Key*[factorial(queryArraysNum)];
    cursize=0;
}

Map::~Map()
{
    delete[] values;
    delete[] keys;
}

bool Map::insert(PredicateArray* key,Value* value)
{
    // std::cout<<"insert 1"<<std::endl;
    int ifexists=exists(key);
        // std::cout<<"insert 2"<<std::endl;

    if(ifexists != -1)
    {
                // std::cout<<"insert exists"<<std::endl;

        delete values[ifexists];
        values[ifexists]=value;
    }
    else
    {
        // std::cout<<"not exists 1"<<std::endl;
        values[cursize]=value;
                // std::cout<<"not exists 2"<<std::endl;

        keys[cursize] = new Key();
        keys[cursize]->KeyArray=key;
                // std::cout<<"not exists 3"<<std::endl;

        cursize++;
    }
    return true;
}
Value* Map::retrieve(PredicateArray* key)
{
    int ifexists=exists(key);
    if(ifexists != -1)
        return values[ifexists];
    return NULL;
}
int Map::exists(PredicateArray* key)
{
    // key->print();
    for(int i=0;i<cursize;i++)
    {
        // keys[i]->KeyArray->print();
        if((*key)==(*this->keys[i]->KeyArray))
            return i;
    }
    return -1;
}
// needs fix
// bool Map::insert(int* key,int keysize,int* value,int valuesize)
// {
//     int ifExistsKey=exists(key,keysize);
//     if(ifExistsKey>=0)
//     {
//         delete values[ifExistsKey];
//         values[ifExistsKey]=new Value(value,valuesize);
//     }
//     else
//     {
//         values[cursize]=new Value(value,valuesize);
//         keys[cursize]=new Key(key,keysize);
//         cursize++;
//     }
// } 
// needs fix
// Value* Map::retrieve(int* key,int size)
// {
//     int ifExistsKey=exists(key,size);
//     if(ifExistsKey>=0)
//     {
//         return values[ifExistsKey];
//     }
// }

// needs fix
// int Map::exists(int* key,int keysize)
// {
//     bool found;
//     for(int i=0;i<cursize;i++)
//     {
//         found=1;
//         if(keys[i]->KeyArray->size==keysize&&keysize>0)
//         {
//             for(int j=0;j<keys[i]->KeyArray->size;j++)
//             {
//                 if(keys[i]->KeyArray->array[j]!=key[j])
//                 {
//                     found=0;
//                     break;
//                 }
//             }
//             if(found)
//                 return i;
//         }
//     }
//     return -1;
// }
void Map::print()
{
    for(int i=0;i<cursize;i++)
    {
        if(values[i]->ValueArray->size<=0)
        {
            std::cout<<"empty value"<<std::endl;
        }
        if(keys[i]->KeyArray->size<=0)
        {
            std::cout<<"empty key"<<std::endl;
        }
        keys[i]->KeyArray->array[0].print(false);
        for(int j=1;j<keys[i]->KeyArray->size;j++)
        {
            std::cout<<", ";
            keys[i]->KeyArray->array[j].print(false);
        }
        std::cout<<"  -->  ";
        values[i]->ValueArray->array[0].print(false);
        for(int j=1;j<values[i]->ValueArray->size;j++)
        {
            std::cout<<", ";
            values[i]->ValueArray->array[j].print(false);
        }
        std::cout<<std::endl;
    }
}
class Ri{
    int array;
    int fieldid;
};
void rec(std::string s,int length,int maxlength,int Rnum)
{
    if(length==maxlength)
        return;
    std::string tmp="";
    for(int i=length;i<Rnum;i++)
    {
        tmp=s+std::to_string(i);
        std::cout<<tmp<<std::endl;
    }
    for(int i=length;i<Rnum;i++)
    {
        tmp=s+std::to_string(i);
        rec(tmp,length+1,maxlength,Rnum);
    }
    
}

void swap(PredicateArray *predicateArray, int a, int b) {
    Predicate tmp = predicateArray->array[a];
    // predicateArray->array[a].fieldId = predicateArray->array[b].fieldId;
    // predicateArray->array[a].predicateArrayId = predicateArray->array[b].predicateArrayId;
    predicateArray->array[a] = predicateArray->array[b];
    // predicateArray->array[b].fieldId = tmp.fieldId;
    // predicateArray->array[b].predicateArrayId = tmp.predicateArrayId;
    predicateArray->array[b] = tmp;
}

int nextIndex = 0;

void getAllPermutations(int size, int permutationSize, PredicateArray* elements, PredicateArray* resultArray) {
    // if size becomes 1 then prints the obtained 
    // permutation 
    if (size == 1) 
    { 
        // printArr(a, n); 
        std::cout<<"index: "<<nextIndex<<std::endl;
        for (int j = 0; j < permutationSize; j++) {
            elements->array[j].print(false);
            std::cout<<" ";
        }
        std::cout<<std::endl;
        resultArray[nextIndex++].init(elements, permutationSize);

        return; 
    } 
  
    for (int i=0; i<size; i++) 
    { 
        getAllPermutations(size - 1, permutationSize, elements, resultArray); 
  
        // if size is odd, swap first and last 
        // element 
        if (size%2==1) {
            swap(elements, 0, size - 1); 
        }
        // If size is even, swap ith and last 
        // element 
        else {
            // swap(a[i], a[size-1]); 
            swap(elements, i, size - 1); 
        }
    } 
    // std::cout<<elements->array[0].predicateArrayId<<"."<<elements->array[0].fieldId<<std::endl;
    //     std::cout<<elements->array[size - 1].predicateArrayId<<"."<<elements->array[size - 1].fieldId<<std::endl;

    // swap(elements, 0, size - 1);
    //     std::cout<<elements->array[0].predicateArrayId<<"."<<elements->array[0].fieldId<<std::endl;
    //     std::cout<<elements->array[size - 1].predicateArrayId<<"."<<elements->array[size - 1].fieldId<<std::endl;
}

int getPermutationsNum(int size) {
    int sum = 0;
    for (int i = 1; i < size; i++) {
        sum += (factorial(size)/factorial(size - i));
    }

    return sum;
}

int getCombinationsNum(int size, int combinationSize) {
    // int sum = 0;
    // for (int i = 1; i < size; i++) {
    //     sum += (factorial(size)/factorial(size - i));
    // }

    // return sum;

    return (factorial(size)/(factorial(combinationSize)*factorial(size - combinationSize)));
}

// void combinationUtil(int arr[], int n, int r, 
//                      int index, int data[], int i); 
  
// The main function that prints all combinations of  
// size r in arr[] of size n. This function mainly 
// uses combinationUtil() 
// void printCombination(int arr[], int n, int r) 
// { 
//     // A temporary array to store all combination 
//     // one by one 
//     int data[r]; 
  
//     // Print all combination using temprary array 'data[]' 
//     combinationUtil(arr, n, r, 0, data, 0); 
// } 

/* arr[]  ---> Input Array 
   n      ---> Size of input array 
   r      ---> Size of a combination to be printed 
   index  ---> Current index in data[] 
   data[] ---> Temporary array to store current combination 
   i      ---> index of current element in arr[]     */
void getCombinations(PredicateArray* elements, int n, int r, int index, PredicateArray* data,
                     PredicateArray* resultArray, int i) 
{ 
    // Current cobination is ready, print it 
    if (index == r) { 
        // for (int j = 0; j < r; j++) 
        //     printf("%d ", data[j]); 
        // printf("\n"); 
            // std::cout<<"1a nextIndex: "<<nextIndex<<std::endl;

        resultArray[nextIndex++].init(data, r);
            // std::cout<<"1b"<<std::endl;

        // for (int i = 0; i < nextIndex; i ++) {
        //     // std::cout<<"i: "<<i<<": ";
        //     resultArray[i].print();
        // }
        return; 
    } 
  
    // When no more elements are there to put in data[] 
    if (i >= n) 
        return; 
  
    // current is included, put next at next location 
    // data[index] = arr[i]; 
    // std::cout<<"1"<<std::endl;

    data->array[index].field1Id = elements->array[i].field1Id;
    data->array[index].predicateArray1Id = elements->array[i].predicateArray1Id;
    data->array[index].field2Id = elements->array[i].field2Id;
    data->array[index].predicateArray2Id = elements->array[i].predicateArray2Id;
    // data.array[index] = elements->array[i];
            // std::cout<<"3"<<std::endl;

    getCombinations(elements, n, r, index + 1, data, resultArray, i + 1); 
  
    // current is excluded, replace it with next 
    // (Note that i+1 is passed, but index is not 
    // changed) 
    getCombinations(elements, n, r, index, data, resultArray, i + 1); 
}

void updateColumnStats(const InputArray* pureInputArray, uint64_t joinFieldId, int predicateArrayId,
                         Value* valueP, Value* newValueP, ColumnStats** filterColumnStatsArray, uint64_t fieldDistinctValuesNumAfterFilter, uint64_t fieldValuesNumBeforeFilter) {
    for (uint64_t j = 0; j < pureInputArray->columnsNum; j++) {
        if (j == joinFieldId)
            continue;
        
        ColumnStats* oldStatsP = &valueP->columnStatsArray[predicateArrayId][j];
        if (oldStatsP->changed == false) { // predicate array is used for the first time
            oldStatsP = &filterColumnStatsArray[predicateArrayId][j];
            if (oldStatsP->changed == false) {
                oldStatsP = &pureInputArray->columnsStats[j];
            }
        }

        ColumnStats* valueStatsP = &newValueP->columnStatsArray[predicateArrayId][j];
        valueStatsP->changed = true;
        ColumnStats* fieldValueStatsP = &newValueP->columnStatsArray[predicateArrayId][joinFieldId];
        
        std::cout<<"fieldValueStatsP->valuesNum: "<<fieldValueStatsP->valuesNum<<", fieldValuesNumBeforeFilter: "<<fieldValuesNumBeforeFilter<<std::endl;
        uint64_t base=1 - (fieldValueStatsP->valuesNum / fieldValuesNumBeforeFilter);
                std::cout<<"aaaa2"<<std::endl;

        uint64_t exponent=oldStatsP->valuesNum / oldStatsP->distinctValuesNum;
        std::cout<<"base: "<<base<<", exponent: "<<exponent<<std::endl;
        valueStatsP->distinctValuesNum=oldStatsP->distinctValuesNum * (1-((uint64_t) pow((double)base,(double)exponent)));
        valueStatsP->valuesNum=fieldValueStatsP->valuesNum;

        // valueStatsP->minValue = UINT64_MAX;
        // valueStatsP->maxValue = 0;

        // for (uint64_t i = 0; i < inputArrayRowIds->rowsNum; i++) {
        //     uint64_t curValue = pureInputArray->columns[j][inputArrayRowIds->columns[0][i]];
        //     if (curValue < valueStatsP->minValue) {
        //         valueStatsP->minValue = curValue;
        //     }
        //     if (curValue > valueStatsP->maxValue) {
        //         valueStatsP->maxValue = curValue;
        //     }
        // }
        
        // valueStatsP->valuesNum = fieldValueStatsP->valuesNum;

        // valueStatsP->calculateDistinctValuesNum(pureInputArray, inputArrayRowIds, j);
                std::cout<<"aaaaaaaaaa"<<std::endl;

        uint64_t powOp = (uint64_t) pow((double) (1 - (fieldValueStatsP->distinctValuesNum / fieldDistinctValuesNumAfterFilter)), (double) (fieldValueStatsP->valuesNum / valueStatsP->distinctValuesNum));
        std::cout<<"powOp: "<<powOp<<std::endl;
        valueStatsP->distinctValuesNum = valueStatsP->distinctValuesNum * (1 - powOp);
    }
}

Value* createJoinTree(Value* valueP, PredicateArray* newPredicateArrayP, int* relationIds, int relationsnum, ColumnStats** filterColumnStatsArray, const InputArray** inputArrays) {
    Predicate* newPredicateP = &newPredicateArrayP->array[newPredicateArrayP->size - 1];
    // std::cout<<"createJoinTree 1"<<std::endl;
    Value* newValueP = new Value();
    newValueP->ValueArray = newPredicateArrayP;
    newValueP->columnStatsArray = new ColumnStats*[relationsnum];
    for (int i = 0; i < relationsnum; i++) {
        newValueP->columnStatsArray[i] = new ColumnStats[inputArrays[relationIds[i]]->columnsNum];
        if (i == newPredicateP->predicateArray1Id || i == newPredicateP->predicateArray2Id) // these ColumnStats* will get updated afterwards
            continue;
        memcpy(newValueP->columnStatsArray[i], valueP->columnStatsArray[i], inputArrays[relationIds[i]]->columnsNum * sizeof(ColumnStats));
    }
    // std::cout<<"createJoinTree 2"<<std::endl;

    uint64_t inputArray1Id = relationIds[newPredicateP->predicateArray1Id];
    uint64_t inputArray2Id = relationIds[newPredicateP->predicateArray2Id];
    uint64_t field1Id = newPredicateP->field1Id;
    uint64_t field2Id = newPredicateP->field2Id;
    uint64_t predicateArray1Id = newPredicateP->predicateArray1Id;
    uint64_t predicateArray2Id = newPredicateP->predicateArray2Id;
    // std::cout<<"createJoinTree 3"<<std::endl;

    ColumnStats* field1OldStatsP = &valueP->columnStatsArray[predicateArray1Id][field1Id];
        // std::cout<<"createJoinTree 3.1"<<std::endl;

    ColumnStats* field2OldStatsP = &valueP->columnStatsArray[predicateArray2Id][field2Id];
            // std::cout<<"createJoinTree 3.2"<<std::endl;
    // std::cout<<"predicateArray1Id: "<<predicateArray1Id<<", field1Id: "<<field1Id<<", predicateArray2Id: "<<predicateArray2Id<<", field2Id: "<<field2Id<<std::endl;
    if (field1OldStatsP->changed == false) { // predicate array of 1st operand is used for the first time
        field1OldStatsP = &filterColumnStatsArray[predicateArray1Id][field1Id];
        if (field1OldStatsP->changed == false) {
            field1OldStatsP = &inputArrays[inputArray1Id]->columnsStats[field1Id];
        }
        // std::cout<<"field1OldStatsP->valuesNum: "<<field1OldStatsP->valuesNum<<std::endl;
    }
            // std::cout<<"createJoinTree 3.3"<<std::endl;
    // valueP->ValueArray->print();
    if (field2OldStatsP->changed == false) { // predicate array of 2nd operand is used for the first time
                // std::cout<<"createJoinTree 3.35"<<std::endl;

        field2OldStatsP = &filterColumnStatsArray[predicateArray2Id][field2Id];
        if (field2OldStatsP->changed == false) {
            field2OldStatsP = &inputArrays[inputArray2Id]->columnsStats[field2Id];
        }
                // std::cout<<"field2OldStatsP->valuesNum: "<<field2OldStatsP->valuesNum<<std::endl;

    }

    if (field1OldStatsP->valuesNum == 0 || field2OldStatsP->valuesNum == 0) {
        newValueP->columnStatsArray[predicateArray1Id][field1Id].changed = true;
        newValueP->columnStatsArray[predicateArray2Id][field2Id].changed = true;
        return newValueP;
    }

            // std::cout<<"createJoinTree 3.4"<<std::endl;

    // std::cout<<"createJoinTree 4"<<std::endl;

    // InputArray* inputArray1RowIds = new InputArray(inputArrays[inputArray1Id]->rowsNum);
    // InputArray* inputArray2RowIds = new InputArray(inputArrays[inputArray2Id]->rowsNum);

    uint64_t minMaxValue = field1OldStatsP->maxValue < field2OldStatsP->maxValue ? field1OldStatsP->maxValue : field2OldStatsP->maxValue;
    uint64_t maxMinValue = field1OldStatsP->minValue > field2OldStatsP->minValue ? field1OldStatsP->minValue : field2OldStatsP->minValue;
    uint64_t n = minMaxValue - maxMinValue + 1;
    // std::cout<<"createJoinTree 5"<<std::endl;

    // filter 1st array
    
    // inputArray1RowIds->filterRowIds(field1Id, 1, minMaxValue, inputArrays[inputArray1Id], 0, inputArray1RowIds->rowsNum);
    // inputArray1RowIds->filterRowIds(field1Id, 0, maxMinValue, inputArrays[inputArray1Id], 0, inputArray1RowIds->rowsNum);
    // >
    // std::cout<<">"<<std::endl;

        // std::cout<<"createJoinTree 6"<<std::endl;

    // filter 2nd array
    // inputArray2RowIds->filterRowIds(field2Id, 1, minMaxValue, inputArrays[inputArray2Id], 0, inputArray2RowIds->rowsNum);
    // inputArray2RowIds->filterRowIds(field2Id, 0, maxMinValue, inputArrays[inputArray2Id], 0, inputArray2RowIds->rowsNum);
    // std::cout<<"createJoinTree 7"<<std::endl;

    // handle field1 and field2 columns
    ColumnStats* field1ValueStatsP = &newValueP->columnStatsArray[predicateArray1Id][field1Id];
    ColumnStats* field2ValueStatsP = &newValueP->columnStatsArray[predicateArray2Id][field2Id];
    field1ValueStatsP->changed = true;
    field2ValueStatsP->changed = true;
    // std::cout<<"createJoinTree 7.1"<<std::endl;

    // handle filter
    uint64_t k1=maxMinValue;
    uint64_t k2=minMaxValue;
    // if(filternum>Stats[array][field].minValue)                
    //     Stats[array][field].minValue=filternum;
    uint64_t field1ValuesNumBeforeFilter = field1OldStatsP->valuesNum;
    // std::cout<<"distinct: "<<field1ValueStatsP->distinctValuesNum<<std::endl;
    field1ValueStatsP->distinctValuesNum=field1OldStatsP->distinctValuesNum* ((k2-k1)/(field1OldStatsP->maxValue-field1OldStatsP->minValue));
    field1ValueStatsP->valuesNum=field1OldStatsP->valuesNum* ((k2-k1)/(field1OldStatsP->maxValue-field1OldStatsP->minValue));
    // std::cout<<"distinct: "<<field1ValueStatsP->distinctValuesNum<<std::endl;

    // if(filternum>Stats[array][field].minValue)                
    //     Stats[array][field].minValue=filternum;
    uint64_t field2ValuesNumBeforeFilter = field2OldStatsP->valuesNum;
    field2ValueStatsP->distinctValuesNum=field2OldStatsP->distinctValuesNum* ((k2-k1)/(field2OldStatsP->maxValue-field2OldStatsP->minValue));
    field2ValueStatsP->valuesNum=field2OldStatsP->valuesNum* ((k2-k1)/(field2OldStatsP->maxValue-field2OldStatsP->minValue));

    // handle join
    field1ValueStatsP->minValue = field2ValueStatsP->minValue = maxMinValue;
    field1ValueStatsP->maxValue = field2ValueStatsP->maxValue = minMaxValue;
    
    field1ValueStatsP->valuesNum = field2ValueStatsP->valuesNum = (field1ValueStatsP->valuesNum * field2ValueStatsP->valuesNum) / n;
    newValueP->cost = field1ValueStatsP->valuesNum;
    // std::cout<<"createJoinTree 7.2"<<std::endl;

    // field1ValueStatsP->calculateDistinctValuesNum(inputArrays[inputArray1Id], inputArray1RowIds, field1Id);
    // field2ValueStatsP->calculateDistinctValuesNum(inputArrays[inputArray2Id], inputArray2RowIds, field2Id);
        // std::cout<<"createJoinTree 7.25"<<std::endl;

    uint64_t field1DistinctValuesNumAfterFilter = field1ValueStatsP->distinctValuesNum;
    uint64_t field2DistinctValuesNumAfterFilter = field2ValueStatsP->distinctValuesNum;
    field1ValueStatsP->distinctValuesNum = field2ValueStatsP->distinctValuesNum = (field1ValueStatsP->distinctValuesNum * field2ValueStatsP->distinctValuesNum) / n;
    // field2ValueStatsP->distinctValuesNum = field1ValueStatsP->distinctValuesNum;
    // std::cout<<"createJoinTree 7.3"<<std::endl;

    // handle rest of columns
    updateColumnStats(inputArrays[inputArray1Id], field1Id, predicateArray1Id, valueP, newValueP, filterColumnStatsArray, field1DistinctValuesNumAfterFilter, field1ValuesNumBeforeFilter);
    updateColumnStats(inputArrays[inputArray2Id], field2Id, predicateArray2Id, valueP, newValueP, filterColumnStatsArray, field2DistinctValuesNumAfterFilter, field2ValuesNumBeforeFilter);
        // std::cout<<"createJoinTree 8"<<std::endl;

    // for (uint64_t j = 0; j < inputArrays[inputArray1Id]->columnsNum; j++) {
    //     if (j == field1Id)
    //         continue;
        
    //     ColumnStats* oldStatsP = &valueP->columnStatsArray[predicateArray1Id][j];
    //     if (oldStatsP->valuesNum == -1) { // predicate array of 1st operand is used for the first time
    //         oldStatsP = &filterColumnStatsArray[predicateArray1Id][j];
    //     }

    //     ColumnStats* valueStatsP = &newValueP->columnStatsArray[predicateArray1Id][field1Id];

    //     valueStatsP->minValue = UINT64_MAX;
    //     valueStatsP->maxValue = 0;

    //     for (uint64_t i = 0; i < inputArray1RowIds->rowsNum; i++) {
    //         uint64_t curValue = inputArrays[inputArray1Id]->columns[j][inputArray1RowIds->columns[0][i]];
    //         if (curValue < valueStatsP->minValue) {
    //             valueStatsP->minValue = curValue;
    //         }
    //         if (curValue > valueStatsP->maxValue) {
    //             valueStatsP->maxValue = curValue;
    //         }
    //     }
                
    //     valueStatsP->valuesNum = field1ValueStatsP->valuesNum;

    //     // if C attribute poy anhkei sto A h sto B ??
    //     valueStatsP->calculateDistinctValuesNum(inputArrays[inputArray1Id], inputArray1RowIds, j);
    //     uint64_t powOp = (uint64_t) pow((double) (1 - (field1ValueStatsP->distinctValuesNum / field1DistinctValuesNumAfterFilter)), (double) (inputArray1RowIds->rowsNum / valueStatsP->distinctValuesNum));
    //     valueStatsP->distinctValuesNum = valueStatsP->distinctValuesNum * (1 - powOp);
    // }

    return newValueP;
}

uint64_t** BestPredicateOrder(uint64_t** currentpreds,int cntr,int relationsnum,int*relationids,const InputArray** inputArrays,ColumnStats** filterColumnStatsArray)
{
    /////////// for testing
    // relationsnum = 3;
    ///////////

    // int Rnum=4;
    // Ri* theRs;
    // for(int i=0;i<Rnum;i++)
    // {   
    //     //MAP.insert(theRs[i]);
    // }
    
    // std::string s="";
    // rec(s,0,Rnum-1,Rnum);
    // for (int i = 1; i < relationsum; i++) {

    // }
    PredicateArray* joinPredicateArray = new PredicateArray(cntr, currentpreds);
    // PredicateArray predicateArray(2);
    // predicateOperandArray.array = new PredicateOperand[4];
    // predicateOperandArray.size = 4;
    // for (int i = 0; i < relationsum; i++) {
        // predicateArray.array[0].predicateArray1Id = 3;
        // predicateArray.array[0].field1Id = 0;
        // predicateArray.array[0].predicateArray2Id = 1;
        // predicateArray.array[0].field2Id = 0;
        // predicateArray.array[1].predicateArray1Id = 2;
        // predicateArray.array[1].field1Id = 1;
        // predicateArray.array[1].predicateArray2Id = 1;
        // predicateArray.array[1].field2Id = 1;
        // predicateArray.array[2].predicateArray1Id = 0;
        // predicateArray.array[2].field1Id = 2;
        // predicateArray.array[2].predicateArray2Id = 3;
        // predicateArray.array[2].field2Id = 2;
        // predicateArray.array[3].predicateArray1Id = 4;
        // predicateArray.array[3].field1Id = 3;
        // predicateArray.array[3].predicateArray2Id = 5;
        // predicateArray.array[3].field2Id = 1;
        // 3 0 1|0.2=1.0&0.1=2.0&0.2>3499
        // predicateArray.array[0].init(0, 2, 1, 0);
        // predicateArray.array[1].init(0, 1, 2, 0);
    // }
        //     PredicateArray* resultArray = new PredicateArray[4];
        // // for (int j = 0; j < curCombinationsNum; j++)
        // //     resultArray[j].init(NULL, i);

        // PredicateArray tempPredicateArray(1);

        // getCombinations(&predicateArray, predicateArray.size, 1, 0, &tempPredicateArray, resultArray, 0);
        // // delete[] tempPredicateArray.array;
        // // std::cout<<"nextindex: "<<nextIndex<<std::endl;
        
        // // used for printing
        // for (int j = 0; j < 4; j++) {
        //     std::cout<<"j: "<<j<<": ";
        //     resultArray[j].print();
        // }
    // std::cout<<"1"<<std::endl;
    Map* bestTreeMap = new Map(relationsnum);
    for (int i = 0; i < relationsnum; i++) {
                    // std::cout<<"outer loop i "<<i<<std::endl;

        PredicateArray* keyValuePredicateArray = new PredicateArray(1);
        keyValuePredicateArray->array[0] = joinPredicateArray->array[i];
        Value* value = new Value(1);
        value->columnStatsArray = new ColumnStats*[relationsnum];
        for (int i = 0; i < relationsnum; i++) {
            // std::cout<<"i "<<i<<std::endl;
            value->columnStatsArray[i] = new ColumnStats[inputArrays[relationids[i]]->columnsNum];
                        // std::cout<<"1, i "<<i<<std::endl;

            // maybe unnecessary ???????????
            // memcpy(value->columnStatsArray[i], filterColumnStatsArray[i], inputArrays[relationids[i]]->columnsNum * sizeof(ColumnStats));

        }
        // 2xcolumns
        value->ValueArray = keyValuePredicateArray;
                        // std::cout<<"5, i "<<i<<std::endl;

        bestTreeMap->insert(keyValuePredicateArray, value);
    }
    // std::cout<<"2"<<std::endl;

    for (int i = 1; i < relationsnum; i++) {
        // std::cout<<"i "<<i<<std::endl;
        nextIndex = 0;
        int curCombinationsNum = getCombinationsNum(relationsnum, i);
                // std::cout<<"1"<<std::endl;

        // std::cout<<curCombinationsNum<<std::endl;
        PredicateArray* resultArray = new PredicateArray[curCombinationsNum];
        // for (int j = 0; j < curCombinationsNum; j++)
        //     resultArray[j].init(NULL, i);
                // std::cout<<"2"<<std::endl;

        PredicateArray tempPredicateArray(i);
        // tempPredicateArray.size = i;
        // tempPredicateArray.array = new Predicate[i];
        getCombinations(joinPredicateArray, joinPredicateArray->size, i, 0, &tempPredicateArray, resultArray, 0);
        // delete[] tempPredicateArray.array;
        // std::cout<<"nextindex: "<<nextIndex<<std::endl;
                // std::cout<<"3"<<std::endl;

        // used for printing
        // for (int j = 0; j < curCombinationsNum; j++) {
        //     std::cout<<"j: "<<j<<": ";
        //     resultArray[j].print();
        // }

        for (int j = 0; j < curCombinationsNum; j++) {
            PredicateArray* curCombination = &resultArray[j];
                    // std::cout<<"j "<<j<<std::endl;
            // std::cout<<"current combination: ";
            // curCombination->print();
            for (int k = 0; k < joinPredicateArray->size; k++) {
                Predicate* curPredicate = &joinPredicateArray->array[k];
                // std::cout<<"k "<<k<<std::endl;
                // curPredicate->print(true);
                if (curCombination->contains(*curPredicate) || !curCombination->isConnectedWith(*curPredicate))
                    continue;
                    // std::cout<<"1 k "<<k<<std::endl;

                // TODO: if (NoCrossProducts && !connected(curPredicate, curCombination))
                //          continue;

                    // std::cout<<"2 k "<<k<<std::endl;

                // S' = S U {Rj}
                // ( S' = newPredicateArray )
                PredicateArray* newPredicateArray = new PredicateArray(curCombination->size + 1);
                newPredicateArray->populate(curCombination);
                // newPredicateArray.array[newPredicateArray.size - 1].fieldId = curPredicate->fieldId;
                // newPredicateArray.array[newPredicateArray.size - 1].predicateArrayId = curPredicate->predicateArrayId;
                newPredicateArray->array[newPredicateArray->size - 1] = (*curPredicate);
                    // std::cout<<"3 k "<<k<<std::endl;

                // Almost done: CurrTree = CreateJoinTree(Map(S), curPredicate) // CreateJoinTree() will create a Value object and it will calculate the cost of the new tree
                Value* curCombinationValue = bestTreeMap->retrieve(curCombination);
                                    // std::cout<<"3.5 k "<<k<<std::endl;
                // curCombinationValue->ValueArray->print();
                                                    // std::cout<<"3.6 k "<<k<<std::endl;

                Value* newValue = createJoinTree(curCombinationValue, newPredicateArray, relationids, relationsnum, filterColumnStatsArray, inputArrays);
                    // std::cout<<"4 k "<<k<<std::endl;
                    
                // TODO: if (Map(S') == NULL || cost(Map(S')) > cost(CurrTree))
                //          Map(S') = CurrTree;
                Value* existingValue = bestTreeMap->retrieve(newPredicateArray);
                if (existingValue == NULL || existingValue->cost > newValue->cost) {
                    bestTreeMap->insert(newPredicateArray, newValue);
                }
            }
        }
        // std::cout<<"before free"<<std::endl;
        delete[] resultArray;
    }
// std::cout<<getPermutationsNum(4)<<std::endl;
    

    // std::cout<<predicateOperandArray.array[0].predicateArrayId<<std::endl;
    // std::cout<<predicateOperandArray.array[0].fieldId<<std::endl;
    // swap(&predicateOperandArray, 0, 1);
    //     std::cout<<predicateOperandArray.array[0].predicateArrayId<<std::endl;
    //     std::cout<<predicateOperandArray.array[0].fieldId<<std::endl;
    uint64_t** newJoinPreds = bestTreeMap->retrieve(joinPredicateArray)->ValueArray->toUintArray();
    delete bestTreeMap;
    return newJoinPreds;
}

uint64_t** OptimizePredicates(uint64_t** currentpreds,int cntr,int relationsum,int*relationids,const InputArray** inputarr)
{
    //******************************missing case for σ A=B ???? is it filter on bestpredicate function?
    // std::cout<<"before optimization"<<std::endl;
    // for(int i=0;i<cntr;i++)
    // {
    //     for(int j=0;j<5;j++)
    //     {
    //         std::cout<<currentpreds[i][j]<<" ";
    //     }
    //     std::cout<<std::endl;
    // }

    // for(int i=0;i<relationsum;i++)
    // {
    //     std::cout<<i<<": "<<relationids[i]<<std::endl;
    // }
    ColumnStats** Stats=new ColumnStats*[relationsum];
    for(int i=0;i<relationsum;i++)
    {
        Stats[i]=new ColumnStats[inputarr[relationids[i]]->columnsNum];
        for(int j=0;j<inputarr[relationids[i]]->columnsNum;j++)
        {
            memcpy(&Stats[i][j],&inputarr[relationids[i]]->columnsStats[j],sizeof(ColumnStats));
        }
    }
    int FiltersNum=0;
    for(int i=0;i<cntr;i++)
    {
        if(currentpreds[i][3]==(uint64_t)-1||(currentpreds[i][0]==currentpreds[i][3]))
            FiltersNum++;
    }
    // std::cout<<FiltersNum<<std::endl;
    uint64_t** Filters=new uint64_t*[FiltersNum];
    uint64_t** NonFilters=new uint64_t*[cntr-FiltersNum]; 
    uint64_t** Final=new uint64_t*[cntr];
    for(int i=0,findx=0,nfindex=0;i<cntr;i++)
    {
        if(currentpreds[i][3]==(uint64_t)-1||(currentpreds[i][0]==currentpreds[i][3]))
        {
            //filter
            Filters[findx]=currentpreds[i];
            findx++;
        }
        else
        {
            NonFilters[nfindex]=currentpreds[i];
            nfindex++;
            //nofilter
        }
    }
    // delete[] currentpreds;
    // std::cout<<"Filters: "<<std::endl;
    // for(int i=0;i<FiltersNum;i++)
    // {
    //     for(int j=0;j<5;j++)
    //     {
    //         std::cout<<Filters[i][j]<<" ";
    //     }
    //     std::cout<<std::endl;
    // }
    // std::cout<<"Non Filters: "<<std::endl;
    // for(int i=0;i<cntr-FiltersNum;i++)
    // {
    //     for(int j=0;j<5;j++)
    //     {
    //         std::cout<<NonFilters[i][j]<<" ";
    //     }
    //     std::cout<<std::endl;
    // }
    FilterStats(Filters,FiltersNum,relationsum,relationids,inputarr,Stats);
    uint64_t** best=BestPredicateOrder(NonFilters,cntr-FiltersNum,relationsum,relationids,inputarr,Stats);
    int next=0;
    for(int i=0;i<FiltersNum;i++)
    {
        Final[next]=Filters[i];
        next++;
    }
    for(int i=0;i<cntr-FiltersNum;i++)
    {
        Final[next]=best[i];
        next++;
    }
    delete[] best;
    // std::cout<<"after optimization"<<std::endl;
    // for(int i=0;i<cntr;i++)
    // {
    //     for(int j=0;j<5;j++)
    //     {
    //         std::cout<<Final[i][j]<<" ";
    //     }
    //     std::cout<<std::endl;
    // }
    return Final;
    // for(int i=0;i<relationsum;i++)
    // {
    //     for(int j=0;j<inputarr[relationids[i]]->columnsNum;j++)
    //     {
    //         std::cout<<inputarr[relationids[i]]->columnsStats[j].distinctValuesNum<<std::endl;
    //         std::cout<<Stats[i][j].distinctValuesNum<<std::endl;
    //     }
    // }

}
void FilterStats(uint64_t** filterpreds,int cntr,int relationsum,int*relationids,const InputArray** inputarr,ColumnStats** Stats)
{
    // std::cout<<"Filterstats"<<std::endl;
    // for(int i=0;i<cntr;i++)
    // {
    //     for(int j=0;j<5;j++)
    //     {
    //         std::cout<<filterpreds[i][j]<<" ";
    //     }
    //     std::cout<<std::endl;
    // }
    std::cout<<"Starting Stats "<<std::endl;
    for(int i=0;i<1;i++)
    {
        std::cout<<"About array: "<<relationids[i]<<" inquery: "<<i<<std::endl;
        for(int j=0;j<inputarr[relationids[i]]->columnsNum;j++)
        {
            std::cout<<"  Column: "<<j<<std::endl;
            std::cout<<"    "<<Stats[i][j].minValue<<std::endl;
            std::cout<<"    "<<Stats[i][j].maxValue<<std::endl;
            std::cout<<"    "<<Stats[i][j].valuesNum<<std::endl;
            std::cout<<"    "<<Stats[i][j].distinctValuesNum<<std::endl;
        }
        std::cout<<std::endl;
    }
    for(int i=0;i<cntr;i++)
    {
        uint64_t array=filterpreds[i][0];
        uint64_t field=filterpreds[i][1];
        uint64_t operation=filterpreds[i][2];
        uint64_t array2ifexists = filterpreds[i][3];
        uint64_t filternum=filterpreds[i][4];
        uint64_t oldF; 
        if(operation==2)
        {
            //=
            // std::cout<<"="<<std::endl;
            if(array2ifexists==(uint64_t)-1)
            {
                Stats[array][field].minValue=filternum;
                Stats[array][field].maxValue=filternum;
                bool found=0;
                for(int j=0;j<inputarr[relationids[array]]->rowsNum;j++)
                {
                    // std::cout<<j<<" of "<<std::endl;
                    if(inputarr[relationids[array]]->columns[field][j]==filternum)
                    {
                        found=1;
                        break;
                    }
                }
                oldF=Stats[array][field].valuesNum;
                if(found)
                {   //ROUND TO NEXT OR PREVIOUS
                    Stats[array][field].valuesNum  =  (double)Stats[array][field].valuesNum  /  (double)Stats[array][field].distinctValuesNum;
                    Stats[array][field].distinctValuesNum=1;
                }
                else
                { 
                    Stats[array][field].valuesNum=0;
                    Stats[array][field].distinctValuesNum=0;
                }
            }
            else
            {
                 if(Stats[array][field].minValue>Stats[array2ifexists][filternum].minValue)
                    Stats[array2ifexists][filternum].minValue=Stats[array][field].minValue;
                else Stats[array][field].minValue=Stats[array2ifexists][filternum].minValue;

                if(Stats[array][field].maxValue<Stats[array2ifexists][filternum].maxValue)
                    Stats[array2ifexists][filternum].maxValue=Stats[array][field].maxValue;
                else Stats[array][field].maxValue=Stats[array2ifexists][filternum].maxValue;

                oldF=Stats[array][field].valuesNum;
                uint64_t n=Stats[array][field].maxValue-Stats[array][field].minValue+1;
                Stats[array][field].valuesNum=Stats[array2ifexists][filternum].valuesNum=(Stats[array][field].valuesNum/n);



                uint64_t base=1-(Stats[array][field].valuesNum / oldF);
                uint64_t exponent=Stats[array][field].valuesNum / Stats[array][field].distinctValuesNum;

                Stats[array][field].distinctValuesNum=Stats[array][filternum].distinctValuesNum=Stats[array][field].distinctValuesNum * (1-(pow(base,exponent)));

            }
            
            Stats[array][field].changed = true;
        }
        else if(operation==0)
        {
            // >
            // std::cout<<">"<<std::endl;
            uint64_t k1=filternum;
            uint64_t k2=Stats[array][field].maxValue;
            oldF=Stats[array][field].valuesNum;
            Stats[array][field].distinctValuesNum=(double)Stats[array][field].distinctValuesNum* (double)((double)((double)k2-(double)k1)/(double)((double)Stats[array][field].maxValue-(double)Stats[array][field].minValue));
            std::cout<<k2<<std::endl;
            std::cout<<k1<<std::endl;
            std::cout<<Stats[array][field].valuesNum<<std::endl;
            std::cout<<k2-k1<<std::endl;

            std::cout<<Stats[array][field].maxValue-Stats[array][field].minValue<<std::endl;
            Stats[array][field].valuesNum=(double)Stats[array][field].valuesNum* (double)((double)((double)k2-(double)k1)/(double)((double)Stats[array][field].maxValue-(double)Stats[array][field].minValue));
            
            if(filternum>Stats[array][field].minValue)                
                Stats[array][field].minValue=filternum;

            Stats[array][field].changed = true;
        }
        else if(operation==1)
        {
            // <
            // std::cout<<"<"<<std::endl;
            uint64_t k1=Stats[array][field].minValue;
            uint64_t k2=filternum;
            oldF=Stats[array][field].valuesNum;
            Stats[array][field].distinctValuesNum=(double)Stats[array][field].distinctValuesNum* (double)((double)((double)k2-(double)k1)/(double)((double)Stats[array][field].maxValue-(double)Stats[array][field].minValue));
            Stats[array][field].valuesNum=(double)Stats[array][field].valuesNum* (double)((double)((double)k2-(double)k1)/(double)((double)Stats[array][field].maxValue-(double)Stats[array][field].minValue));

            if(filternum<Stats[array][field].maxValue)
                Stats[array][field].maxValue=filternum;

            Stats[array][field].changed = true;
        }
        for(int j=0;j<inputarr[relationids[array]]->columnsNum;j++)
        {
            if(j==field||(array2ifexists!=(uint64_t)-1&&j==filternum))
                continue;
            //min same
            //max same
            uint64_t base=1-((double)Stats[array][field].valuesNum /(double) oldF);
            uint64_t exponent=(double)Stats[array][j].valuesNum / (double)Stats[array][j].distinctValuesNum;
            Stats[array][j].distinctValuesNum=(double)Stats[array][j].distinctValuesNum * (double)(1-(pow(base,exponent)));
            Stats[array][j].valuesNum=Stats[array][field].valuesNum;
            Stats[array][j].changed = true;
        }
        
    }
    std::cout<<"Ending Stats "<<std::endl;
    for(int i=0;i<1;i++)
    {
        std::cout<<"About array: "<<relationids[i]<<" inquery: "<<i<<std::endl;
        for(int j=0;j<inputarr[relationids[i]]->columnsNum;j++)
        {
            std::cout<<"  Column: "<<j<<std::endl;
            std::cout<<"    "<<Stats[i][j].minValue<<std::endl;
            std::cout<<"    "<<Stats[i][j].maxValue<<std::endl;
            std::cout<<"    "<<Stats[i][j].valuesNum<<std::endl;
            std::cout<<"    "<<Stats[i][j].distinctValuesNum<<std::endl;
        }
        std::cout<<std::endl;
    }

}