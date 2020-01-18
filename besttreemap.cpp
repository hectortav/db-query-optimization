#include "besttreemap.h"

pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;

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
    this->array = new Predicate[size];

    if (predicateArray != NULL)
        memcpy(this->array, predicateArray->array, sizeof(Predicate)*size);
    this->size = size;
}

bool PredicateArray::contains(Predicate predicate) {
    for (int i = 0; i < size; i++) {
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

bool PredicateArray::isConnected() {
    return true;
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
    if(this->size!=array.size)
        return 0;

    for(int i=0;i<this->size;i++)
    {
        int found=0;
        for(int j=0;j<array.size;j++)
        {
            if(this->array[i]==array.array[j])
            {
                found=1;
                break;
            }
        }
        if(found==0)
            return 0;
    }
    
    for(int i=0;i<array.size;i++)
    {
        int found=0;
        for(int j=0;j<this->size;j++)
        {
            if(this->array[j]==array.array[i])
            {
                found=1;
                break;
            }
        }
        if(found==0)
            return 0;
    }
    
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
bool Predicate::issame(Predicate& prdct)
{
    if(*this==prdct)
        return true;
    Predicate p1;
    p1.init(prdct.predicateArray2Id,prdct.field2Id,prdct.predicateArray1Id,prdct.field1Id);
    if(*this==p1)
        return true;
    
    return false;
}
Key::Key(int sz)
{
    this->KeyArray=new PredicateArray(sz);

}

Key::~Key()
{
    delete this->KeyArray;
}

Value::Value(int sz)
{
    this->ValueArray=new PredicateArray(sz);
    this->stats=NULL;
    this->columnStatsArray = NULL;
    this->cost=0;
    this->ColumnStatsArraySize=0;
}
Value::~Value()
{
    delete this->ValueArray;    
    for(int i=0;i<this->ColumnStatsArraySize;i++)
        delete[] this->columnStatsArray[i];

    delete[] this->columnStatsArray;
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

int getCombinationsNum(int size, int combinationSize);

Map::Map(int queryArraysNum)
{
    uint64_t size = 0;
    for (int i = 1; i <= queryArraysNum; i++) {
        size += getCombinationsNum(queryArraysNum, i);
    }
    values=new Value*[size];
    keys=new Key*[size];
    for (uint64_t i = 0; i < size; i++) {
        values[i] = NULL;
        keys[i] = NULL;
    }
    cursize=0;
}

Map::~Map()
{
    for (uint64_t i = 0; i < cursize; i++) {
        if (values[i] != NULL)
            delete values[i];
        if (keys[i] != NULL)
            delete keys[i];
    }
    delete[] values;
    delete[] keys;
}

bool Map::insert(PredicateArray* key,Value* value)
{

    int ifexists=exists(key);
    if(ifexists != -1)
    {
        delete values[ifexists];       
        values[ifexists]=NULL;         

        values[ifexists]=value;
        delete key;
    }
    else
    {
        values[cursize]=value;
        keys[cursize] = new Key();
        keys[cursize]->KeyArray=key;

        cursize++;
    }
    return true;
}
Value* Map::retrieve(PredicateArray* key)
{
    int ifexists=exists(key);
    if(ifexists != -1)
{
        return values[ifexists];
}
        
    return NULL;
}
int Map::exists(PredicateArray* key)
{
    for(int i=0;i<cursize;i++)
    {
        if((*key)==(*this->keys[i]->KeyArray))
            return i;
    }
    return -1;
}
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
    predicateArray->array[a] = predicateArray->array[b];
    predicateArray->array[b] = tmp;
}

int getPermutationsNum(int size) {
    int sum = 0;
    for (int i = 1; i < size; i++) {
        sum += (factorial(size)/factorial(size - i));
    }

    return sum;
}

int getCombinationsNum(int size, int combinationSize) {

    return (factorial(size)/(factorial(combinationSize)*factorial(size - combinationSize)));
}

/* arr[]  ---> Input Array 
   n      ---> Size of input array 
   r      ---> Size of a combination to be printed 
   index  ---> Current index in data[] 
   data[] ---> Temporary array to store current combination 
   i      ---> index of current element in arr[]     */
void getCombinations(PredicateArray* elements, int n, int r, int index, PredicateArray* data,
                     PredicateArray* resultArray, int i, int& nextIndex) 
{ 
    // Current cobination is ready, print it 
    if (index == r) { 

        resultArray[nextIndex++].init(data, r);
        return; 
    } 
  
    // When no more elements are there to put in data[] 
    if (i >= n) 
        return; 
  
    // current is included, put next at next location 

    data->array[index] = elements->array[i];

    getCombinations(elements, n, r, index + 1, data, resultArray, i + 1, nextIndex); 
  
    // current is excluded, replace it with next 
    getCombinations(elements, n, r, index, data, resultArray, i + 1, nextIndex); 
}

void updateColumnStats(const InputArray* pureInputArray, uint64_t joinFieldId, int predicateArrayId,
                         Value* valueP, Value* newValueP, ColumnStats** filterColumnStatsArray, uint64_t fieldDistinctValuesNumAfterFilter, uint64_t fieldValuesNumBeforeFilter) {
    for (uint64_t j = 0; j < pureInputArray->columnsNum; j++) {
        if (j == joinFieldId)
            continue;
        
        ColumnStats* oldStatsP = &valueP->columnStatsArray[predicateArrayId][j];
        if (oldStatsP->changed == false) { // predicate array is used for the first time
            oldStatsP = &filterColumnStatsArray[predicateArrayId][j];
        }

        ColumnStats* valueStatsP = &newValueP->columnStatsArray[predicateArrayId][j];
        valueStatsP->changed = true;
        ColumnStats* fieldValueStatsP = &newValueP->columnStatsArray[predicateArrayId][joinFieldId];
        double base=(double)1 - (double)((double)fieldValueStatsP->valuesNum / (double)fieldValuesNumBeforeFilter);
        if (base < 0) {
            base = 0;
        }
        double exponent=(double)oldStatsP->valuesNum / (double)oldStatsP->distinctValuesNum;
        valueStatsP->distinctValuesNum= (double)oldStatsP->distinctValuesNum * (double)((double)((double)1-(pow(base,(double)exponent))));
        valueStatsP->valuesNum=fieldValueStatsP->valuesNum;
        double powOp = pow((double) ((double)1 - (double)((double)fieldValueStatsP->distinctValuesNum / (double)fieldDistinctValuesNumAfterFilter)), (double) ((double)fieldValueStatsP->valuesNum / (double)valueStatsP->distinctValuesNum));
        valueStatsP->distinctValuesNum = valueStatsP->distinctValuesNum * ((double)1 - powOp);
    }
}

void updateValueStats(Value* valueP, Value* newValueP, Predicate* newPredicateP, int* relationIds, ColumnStats** filterColumnStatsArray, const InputArray** inputArrays) {
    uint64_t inputArray1Id = relationIds[newPredicateP->predicateArray1Id];
    uint64_t inputArray2Id = relationIds[newPredicateP->predicateArray2Id];
    uint64_t field1Id = newPredicateP->field1Id;
    uint64_t field2Id = newPredicateP->field2Id;
    uint64_t predicateArray1Id = newPredicateP->predicateArray1Id;
    uint64_t predicateArray2Id = newPredicateP->predicateArray2Id;

    ColumnStats* field1OldStatsP = &valueP->columnStatsArray[predicateArray1Id][field1Id];

    ColumnStats* field2OldStatsP = &valueP->columnStatsArray[predicateArray2Id][field2Id];
    if (field1OldStatsP->changed == false) { // predicate array of 1st operand is used for the first time
        field1OldStatsP = &filterColumnStatsArray[predicateArray1Id][field1Id];
    }
    if (field2OldStatsP->changed == false) { // predicate array of 2nd operand is used for the first time

        field2OldStatsP = &filterColumnStatsArray[predicateArray2Id][field2Id];

    }

    if (field1OldStatsP->valuesNum == 0 || field2OldStatsP->valuesNum == 0) {
        newValueP->columnStatsArray[predicateArray1Id][field1Id].changed = true;
        newValueP->columnStatsArray[predicateArray2Id][field2Id].changed = true;
        return;
    }
    uint64_t minMaxValue = field1OldStatsP->maxValue < field2OldStatsP->maxValue ? field1OldStatsP->maxValue : field2OldStatsP->maxValue;
    uint64_t maxMinValue = field1OldStatsP->minValue > field2OldStatsP->minValue ? field1OldStatsP->minValue : field2OldStatsP->minValue;
    if (maxMinValue > minMaxValue) {
        minMaxValue = maxMinValue;
    }
    uint64_t n = minMaxValue - maxMinValue + 1;

    // handle field1 and field2 columns
    ColumnStats* field1ValueStatsP = &newValueP->columnStatsArray[predicateArray1Id][field1Id];
    ColumnStats* field2ValueStatsP = &newValueP->columnStatsArray[predicateArray2Id][field2Id];
    field1ValueStatsP->changed = true;
    field2ValueStatsP->changed = true;

    // handle filter
    uint64_t k1=maxMinValue;
    uint64_t k2=minMaxValue;
    uint64_t field1ValuesNumBeforeFilter = field1OldStatsP->valuesNum;
    field1ValueStatsP->distinctValuesNum=field1OldStatsP->distinctValuesNum* ((double)(k2-k1)/(double)(field1OldStatsP->maxValue-field1OldStatsP->minValue));
    field1ValueStatsP->valuesNum=field1OldStatsP->valuesNum* ((double)(k2-k1)/(double)(field1OldStatsP->maxValue-field1OldStatsP->minValue));
    uint64_t field2ValuesNumBeforeFilter = field2OldStatsP->valuesNum;
    field2ValueStatsP->distinctValuesNum=field2OldStatsP->distinctValuesNum* ((double)(k2-k1)/(double)(field2OldStatsP->maxValue-field2OldStatsP->minValue));
    field2ValueStatsP->valuesNum=field2OldStatsP->valuesNum* ((double)(k2-k1)/(double)(field2OldStatsP->maxValue-field2OldStatsP->minValue));

    // handle join
    field1ValueStatsP->minValue = field2ValueStatsP->minValue = maxMinValue;
    field1ValueStatsP->maxValue = field2ValueStatsP->maxValue = minMaxValue;
    
    field1ValueStatsP->valuesNum = field2ValueStatsP->valuesNum = (double)(field1ValueStatsP->valuesNum * field2ValueStatsP->valuesNum) / (double) n;
    newValueP->cost = field1ValueStatsP->valuesNum;

    uint64_t field1DistinctValuesNumAfterFilter = field1ValueStatsP->distinctValuesNum;
    uint64_t field2DistinctValuesNumAfterFilter = field2ValueStatsP->distinctValuesNum;
    field1ValueStatsP->distinctValuesNum = field2ValueStatsP->distinctValuesNum = (field1ValueStatsP->distinctValuesNum * field2ValueStatsP->distinctValuesNum) / n;

    // handle rest of columns

    updateColumnStats(inputArrays[inputArray1Id], field1Id, predicateArray1Id, valueP, newValueP, filterColumnStatsArray, field1DistinctValuesNumAfterFilter, field1ValuesNumBeforeFilter);
    updateColumnStats(inputArrays[inputArray2Id], field2Id, predicateArray2Id, valueP, newValueP, filterColumnStatsArray, field2DistinctValuesNumAfterFilter, field2ValuesNumBeforeFilter);
}

Value* createJoinTree(Value* valueP, PredicateArray* newPredicateArrayP, int* relationIds, int relationsnum, ColumnStats** filterColumnStatsArray, const InputArray** inputArrays) {
    Predicate* newPredicateP = &newPredicateArrayP->array[newPredicateArrayP->size - 1];
    Value* newValueP = new Value();
    newValueP->ValueArray = new PredicateArray(newPredicateArrayP->size);
    memcpy(newValueP->ValueArray->array, newPredicateArrayP->array, sizeof(Predicate)*newPredicateArrayP->size);

    newValueP->columnStatsArray = new ColumnStats*[relationsnum];
    newValueP->ColumnStatsArraySize=relationsnum;

    for (int i = 0; i < relationsnum; i++) {

        newValueP->columnStatsArray[i] = new ColumnStats[inputArrays[relationIds[i]]->columnsNum];

        if (i == newPredicateP->predicateArray1Id || i == newPredicateP->predicateArray2Id) // these ColumnStats* will get updated afterwards
            continue;
        memcpy(newValueP->columnStatsArray[i], valueP->columnStatsArray[i], inputArrays[relationIds[i]]->columnsNum * sizeof(ColumnStats));

    }

    updateValueStats(valueP, newValueP, newPredicateP, relationIds, filterColumnStatsArray, inputArrays);

    return newValueP;
}

uint64_t** BestPredicateOrder(uint64_t** currentpreds,int cntr,int relationsnum,int*relationids,const InputArray** inputArrays,ColumnStats** filterColumnStatsArray)
{
    PredicateArray* joinPredicateArray = new PredicateArray(cntr, currentpreds);

    Map* bestTreeMap = new Map(cntr);
    for (int i = 0; i < cntr; i++) {

        PredicateArray* keyValuePredicateArray = new PredicateArray(1);
        keyValuePredicateArray->array[0] = joinPredicateArray->array[i];
        Value* value = new Value(1);
        value->columnStatsArray = new ColumnStats*[relationsnum];
        value->ColumnStatsArraySize=relationsnum;
        for (int i = 0; i < relationsnum; i++) {
            value->columnStatsArray[i] = new ColumnStats[inputArrays[relationids[i]]->columnsNum];
        }
        memcpy(value->ValueArray->array, keyValuePredicateArray->array, sizeof(Predicate));
        
        updateValueStats(value, value, &joinPredicateArray->array[i], relationids, filterColumnStatsArray, inputArrays);

        bestTreeMap->insert(keyValuePredicateArray, value);
    }
    int nextIndex;
    for (int i = 1; i < cntr; i++) {
        nextIndex = 0;
        int curCombinationsNum = getCombinationsNum(cntr, i);
        PredicateArray* resultArray = new PredicateArray[curCombinationsNum];

        PredicateArray tempPredicateArray(i);
        getCombinations(joinPredicateArray, joinPredicateArray->size, i, 0, &tempPredicateArray, resultArray, 0, nextIndex);

        for (int j = 0; j < curCombinationsNum; j++) {
            PredicateArray* curCombination = &resultArray[j];
            for (int k = 0; k < joinPredicateArray->size; k++) {
                Predicate* curPredicate = &joinPredicateArray->array[k];
                if (curCombination->contains(*curPredicate) || !curCombination->isConnectedWith(*curPredicate))
                    continue;

                PredicateArray* newPredicateArray = new PredicateArray(curCombination->size + 1);
                newPredicateArray->populate(curCombination);
                newPredicateArray->array[newPredicateArray->size - 1] = (*curPredicate);

                Value* curCombinationValue = bestTreeMap->retrieve(curCombination);
                if (curCombinationValue == NULL) {
                    delete newPredicateArray;
                    continue;
                }

                Value* newValue = createJoinTree(curCombinationValue, newPredicateArray, relationids, relationsnum, filterColumnStatsArray, inputArrays);
                Value* existingValue = bestTreeMap->retrieve(newPredicateArray);
                if (existingValue == NULL || existingValue->cost > newValue->cost) {
                    bestTreeMap->insert(newPredicateArray, newValue);
                } else {
                    delete newPredicateArray;
                    delete newValue;
                }
            }
        }
        delete[] resultArray;
    }
    uint64_t** newJoinPreds = bestTreeMap->retrieve(joinPredicateArray)->ValueArray->toUintArray();
    delete bestTreeMap;
    delete joinPredicateArray;
    return newJoinPreds;
}

uint64_t** OptimizePredicates(uint64_t** currentpreds,int& cntr,int relationsum,int*relationids,const InputArray** inputarr)
{
    int FiltersNum=0;
    int NonFiltersNum=0;
    for(int i=0;i<cntr;i++)
    {
        if(currentpreds[i][3]==(uint64_t)-1||(currentpreds[i][0]==currentpreds[i][3]))
            FiltersNum++;
    }
    NonFiltersNum=cntr-FiltersNum;
    if(NonFiltersNum<=1)
    {
        return noopt(currentpreds,cntr);
    }
    ColumnStats** Stats=new ColumnStats*[relationsum];
    for(int i=0;i<relationsum;i++)
    {
        Stats[i]=new ColumnStats[inputarr[relationids[i]]->columnsNum];
        for(int j=0;j<inputarr[relationids[i]]->columnsNum;j++)
        {
            memcpy(&Stats[i][j],&inputarr[relationids[i]]->columnsStats[j],sizeof(ColumnStats));
        }
    }
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

    PredicateArray* Preds=new PredicateArray(NonFiltersNum,NonFilters);
    PredicateArray* NewPreds=new PredicateArray(NonFiltersNum);
    for(int i=0;i<NonFiltersNum;i++)
    {
        delete[] NonFilters[i];
    }
    delete[] NonFilters;

    NonFiltersNum=0;
    for(int i=0;i<Preds->size;i++)
    {
        bool found=0;
        for(int j=0;j<NonFiltersNum;j++)
        {
            if(Preds->array[i].issame(Preds->array[j]))
                found=1; 
        }
        if(!found)
            NewPreds->array[NonFiltersNum++]=Preds->array[i];


    }
    NewPreds->size=NonFiltersNum;
    delete Preds;
    uint64_t** newpreds=NewPreds->toUintArray();
    delete NewPreds;
    cntr=NonFiltersNum+FiltersNum;
    delete[] currentpreds;
    FilterStats(Filters,FiltersNum,relationsum,relationids,inputarr,Stats);
    uint64_t** best;
    best=BestPredicateOrder(newpreds,NonFiltersNum,relationsum,relationids,inputarr,Stats);
    for(int i=0;i<NonFiltersNum;i++)
        delete[] newpreds[i];
    delete[] newpreds;
    int next=0;
    for(int i=0;i<FiltersNum;i++)
    {
        Final[next]=Filters[i];
        next++;
    }
    for(int i=0;i<NonFiltersNum;i++)
    {
        Final[next]=best[i];
        next++;
    }
    delete[] Filters;
    delete[] best;
    for(int i=0;i<relationsum;i++)
    {
        delete[] Stats[i];
    }
    delete[] Stats;
    return Final;

}
void FilterStats(uint64_t** filterpreds,int cntr,int relationsum,int*relationids,const InputArray** inputarr,ColumnStats** Stats)
{
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
            if(array2ifexists==(uint64_t)-1)
            {
                Stats[array][field].minValue=filternum;
                Stats[array][field].maxValue=filternum;
                bool found=0;
                for(int j=0;j<inputarr[relationids[array]]->rowsNum;j++)
                {
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



                double base=(double)1-(double)((double)Stats[array][field].valuesNum / (double)oldF);
                double exponent=(double)Stats[array][field].valuesNum / (double)Stats[array][field].distinctValuesNum;

                Stats[array][field].distinctValuesNum=Stats[array][filternum].distinctValuesNum=Stats[array][field].distinctValuesNum * (double)((double)1-(double)(pow((double)base,(double)exponent)));

            }
            
            Stats[array][field].changed = true;
        }
        else if(operation==0)
        {
            // >
            uint64_t k1=filternum;
            uint64_t k2=Stats[array][field].maxValue;
            oldF=Stats[array][field].valuesNum;
            Stats[array][field].distinctValuesNum=(double)Stats[array][field].distinctValuesNum* (double)((double)((double)k2-(double)k1)/(double)((double)Stats[array][field].maxValue-(double)Stats[array][field].minValue));
            Stats[array][field].valuesNum=(double)Stats[array][field].valuesNum* (double)((double)((double)k2-(double)k1)/(double)((double)Stats[array][field].maxValue-(double)Stats[array][field].minValue));
            
            if(filternum>Stats[array][field].minValue)                
                Stats[array][field].minValue=filternum;

            Stats[array][field].changed = true;
        }
        else if(operation==1)
        {
            // <
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
            double base=(double)1-(double)((double)Stats[array][field].valuesNum /(double) oldF);
            double exponent=(double)Stats[array][j].valuesNum / (double)Stats[array][j].distinctValuesNum;
            Stats[array][j].distinctValuesNum=(double)Stats[array][j].distinctValuesNum * (double)((double)1-(double)(pow(base,exponent)));
            Stats[array][j].valuesNum=Stats[array][field].valuesNum;
            Stats[array][j].changed = true;
        }
        
    }

}