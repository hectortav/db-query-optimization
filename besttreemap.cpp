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

bool Map::insert(PredicateArray* key,PredicateArray* value)
{
    int ifexists=exists(key);
    if(ifexists)
    {
        delete values[ifexists];
        values[ifexists]->ValueArray=value;
        values[ifexists]->stats=NULL;//
    }
    else
    {
        values[cursize]->ValueArray=value;
        values[cursize]->stats=NULL;//
        keys[cursize]->KeyArray=key;
        cursize++;
    }
}
Value* Map::retrieve(PredicateArray* key)
{
    int ifexists=exists(key);
    if(ifexists)
        return values[ifexists];
}
int Map::exists(PredicateArray* key)
{
    for(int i=0;i<cursize;i++)
    {
        if(key==this->keys[i]->KeyArray)
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

uint64_t** BestPredicateOrder(uint64_t** currentpreds,int cntr,int relationsum,int*relationids,const InputArray** inputarr,ColumnStats** Stats )
{
    int Rnum=4;
    // Ri* theRs;
    // for(int i=0;i<Rnum;i++)
    // {   
    //     //MAP.insert(theRs[i]);
    // }
    
    std::string s="";
    // rec(s,0,Rnum-1,Rnum);
    // for (int i = 1; i < relationsum; i++) {

    // }

    PredicateArray predicateArray(4);
    // predicateOperandArray.array = new PredicateOperand[4];
    // predicateOperandArray.size = 4;
    // for (int i = 0; i < relationsum; i++) {
        predicateArray.array[0].predicateArray1Id = 3;
        predicateArray.array[0].field1Id = 0;
        predicateArray.array[0].predicateArray2Id = 1;
        predicateArray.array[0].field2Id = 0;
        predicateArray.array[1].predicateArray1Id = 2;
        predicateArray.array[1].field1Id = 1;
        predicateArray.array[1].predicateArray2Id = 1;
        predicateArray.array[1].field2Id = 1;
        predicateArray.array[2].predicateArray1Id = 0;
        predicateArray.array[2].field1Id = 2;
        predicateArray.array[2].predicateArray2Id = 3;
        predicateArray.array[2].field2Id = 2;
        predicateArray.array[3].predicateArray1Id = 4;
        predicateArray.array[3].field1Id = 3;
        predicateArray.array[3].predicateArray2Id = 5;
        predicateArray.array[3].field2Id = 1;
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

    for (int i = 1; i < 4; i++) {
        nextIndex = 0;
        int curCombinationsNum = getCombinationsNum(4, i);
        // std::cout<<curCombinationsNum<<std::endl;
        PredicateArray* resultArray = new PredicateArray[curCombinationsNum];
        // for (int j = 0; j < curCombinationsNum; j++)
        //     resultArray[j].init(NULL, i);

        PredicateArray tempPredicateArray(i);
        // tempPredicateArray.size = i;
        // tempPredicateArray.array = new Predicate[i];
        getCombinations(&predicateArray, predicateArray.size, i, 0, &tempPredicateArray, resultArray, 0);
        // delete[] tempPredicateArray.array;
        // std::cout<<"nextindex: "<<nextIndex<<std::endl;

        // used for printing
        // for (int j = 0; j < curCombinationsNum; j++) {
        //     std::cout<<"j: "<<j<<": ";
        //     resultArray[j].print();
        // }

        for (int j = 0; j < curCombinationsNum; j++) {
            PredicateArray* curCombination = &resultArray[j];
            for (int k = 0; k < predicateArray.size; k++) {
                Predicate* curPredicate = &predicateArray.array[k];
                if (curCombination->contains(*curPredicate) || curCombination->isConnectedWith(*curPredicate))
                    continue;

                // TODO: if (NoCrossProducts && !connected(curPredicate, curCombination))
                //          continue;

                // TODO: CurrTree = CreateJoinTree(Map(S), curPredicate) // CreateJoinTree() will create a Value object and it will calculate the cost of the new tree

                // S' = S U {Rj}
                // ( S' = newPredicateArray )
                PredicateArray newPredicateArray(curCombination->size + 1);
                newPredicateArray.populate(curCombination);
                // newPredicateArray.array[newPredicateArray.size - 1].fieldId = curPredicate->fieldId;
                // newPredicateArray.array[newPredicateArray.size - 1].predicateArrayId = curPredicate->predicateArrayId;
                newPredicateArray.array[newPredicateArray.size - 1] = (*curPredicate);

                // TODO: if (Map(S') == NULL || cost(Map(S')) > cost(CurrTree))
                //          Map(S') = CurrTree;
            }
        }

        delete[] resultArray;
    }
// std::cout<<getPermutationsNum(4)<<std::endl;
    

    // std::cout<<predicateOperandArray.array[0].predicateArrayId<<std::endl;
    // std::cout<<predicateOperandArray.array[0].fieldId<<std::endl;
    // swap(&predicateOperandArray, 0, 1);
    //     std::cout<<predicateOperandArray.array[0].predicateArrayId<<std::endl;
    //     std::cout<<predicateOperandArray.array[0].fieldId<<std::endl;

}

uint64_t** OptimizePredicates(uint64_t** currentpreds,int cntr,int relationsum,int*relationids,const InputArray** inputarr)
{
    //******************************missing case for σ A=B ???? is it filter on bestpredicate function?
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
        if(currentpreds[i][3]==(uint64_t)-1)
            FiltersNum++;
    }
    // std::cout<<FiltersNum<<std::endl;
    uint64_t** Filters=new uint64_t*[FiltersNum];
    uint64_t** NonFilters=new uint64_t*[cntr-FiltersNum]; 
    for(int i=0,findx=0,nfindex=0;i<cntr;i++)
    {
        if(currentpreds[i][3]==(uint64_t)-1)
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
    BestPredicateOrder(NonFilters,cntr-FiltersNum,relationsum,relationids,inputarr,Stats);
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
    // std::cout<<"Starting Stats "<<std::endl;
    // for(int i=0;i<relationsum;i++)
    // {
    //     std::cout<<"About array: "<<relationids[i]<<" inquery: "<<i<<std::endl;
    //     for(int j=0;j<inputarr[relationids[i]]->columnsNum;j++)
    //     {
    //         std::cout<<"  Column: "<<j<<std::endl;
    //         std::cout<<"    "<<Stats[i][j].minValue<<std::endl;
    //         std::cout<<"    "<<Stats[i][j].maxValue<<std::endl;
    //         std::cout<<"    "<<Stats[i][j].valuesNum<<std::endl;
    //         std::cout<<"    "<<Stats[i][j].distinctValuesNum<<std::endl;
    //     }
    //     std::cout<<std::endl;
    // }
    for(int i=0;i<cntr;i++)
    {
        uint64_t array=filterpreds[i][0];
        uint64_t field=filterpreds[i][1];
        uint64_t operation=filterpreds[i][2];
        uint64_t filternum=filterpreds[i][4];
        uint64_t oldF; 
        if(operation==2)
        {
            //=
            // std::cout<<"="<<std::endl;
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
                Stats[array][field].valuesNum  =  Stats[array][field].valuesNum  /  Stats[array][field].distinctValuesNum;
                Stats[array][field].distinctValuesNum=1;
            }
            else
            { 
                Stats[array][field].valuesNum=0;
                Stats[array][field].distinctValuesNum=0;
            }
            
        }
        else if(operation==0)
        {
            // >
            // std::cout<<">"<<std::endl;
            uint64_t k1=filternum;
            uint64_t k2=Stats[array][field].maxValue;
            oldF=Stats[array][field].valuesNum;
            Stats[array][field].distinctValuesNum=Stats[array][field].distinctValuesNum* ((k2-k1)/(Stats[array][field].maxValue-Stats[array][field].minValue));
            Stats[array][field].valuesNum=Stats[array][field].valuesNum* ((k2-k1)/(Stats[array][field].maxValue-Stats[array][field].minValue));

            if(filternum>Stats[array][field].minValue)                
                Stats[array][field].minValue=filternum;

        }
        else if(operation==1)
        {
            // <
            // std::cout<<"<"<<std::endl;
            uint64_t k1=Stats[array][field].minValue;
            uint64_t k2=filternum;
            oldF=Stats[array][field].valuesNum;
            Stats[array][field].distinctValuesNum=Stats[array][field].distinctValuesNum* ((k2-k1)/(Stats[array][field].maxValue-Stats[array][field].minValue));
            Stats[array][field].valuesNum=Stats[array][field].valuesNum* ((k2-k1)/(Stats[array][field].maxValue-Stats[array][field].minValue));

            if(filternum<Stats[array][field].maxValue)
                Stats[array][field].maxValue=filternum;

        }
        for(int j=0;j<inputarr[relationids[array]]->columnsNum;j++)
        {
            if(j==field)
                continue;
            //min same
            //max same
            uint64_t base=1-(Stats[array][field].valuesNum / oldF);
            uint64_t exponent=Stats[array][j].valuesNum / Stats[array][j].distinctValuesNum;
            Stats[array][j].distinctValuesNum=Stats[array][j].distinctValuesNum * (1-(pow(base,exponent)));
            Stats[array][j].valuesNum=Stats[array][field].valuesNum;
        }
        
    }
    // std::cout<<"Ending Stats "<<std::endl;
    // for(int i=0;i<relationsum;i++)
    // {
    //     std::cout<<"About array: "<<relationids[i]<<" inquery: "<<i<<std::endl;
    //     for(int j=0;j<inputarr[relationids[i]]->columnsNum;j++)
    //     {
    //         std::cout<<"  Column: "<<j<<std::endl;
    //         std::cout<<"    "<<Stats[i][j].minValue<<std::endl;
    //         std::cout<<"    "<<Stats[i][j].maxValue<<std::endl;
    //         std::cout<<"    "<<Stats[i][j].valuesNum<<std::endl;
    //         std::cout<<"    "<<Stats[i][j].distinctValuesNum<<std::endl;
    //     }
    //     std::cout<<std::endl;
    // }

}