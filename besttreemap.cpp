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

PredicateOperandArray::PredicateOperandArray() {
    this->size = 0;
    this->array = NULL;
}

PredicateOperandArray::PredicateOperandArray(int size)
{
    this->size = size;
    this->array = new PredicateOperand[size];
}
PredicateOperandArray::~PredicateOperandArray()
{
    delete[] array;
    array = NULL;
}

void PredicateOperandArray::init(PredicateOperandArray* operandArray, int size) {
    // std::cout<<"b size: "<<size<<std::endl;
    this->array = new PredicateOperand[size];
        // std::cout<<"e"<<std::endl;

    if (operandArray != NULL)
        memcpy(this->array, operandArray->array, sizeof(PredicateOperand)*size);
    this->size = size;
}

Key::Key(int* arr,int sz)
{
    this->KeyArray=new PredicateOperandArray(sz);
    // this->KeyArray->array=arr;
    // this->KeyArray->size=sz;
}

Key::~Key()
{
    delete[] this->KeyArray;
}

Value::Value(int* arr, int sz)
{
    this->ValueArray=new PredicateOperandArray(sz);
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
        std::cout<<keys[i]->KeyArray->array[0].predicateArrayId<<"."<<keys[i]->KeyArray->array[0].fieldId;
        for(int j=1;j<keys[i]->KeyArray->size;j++)
        {
            std::cout<<", "<<keys[i]->KeyArray->array[j].predicateArrayId<<"."<<keys[i]->KeyArray->array[j].fieldId;
        }
        std::cout<<"  -->  ";
        std::cout<<values[i]->ValueArray->array[0].predicateArrayId<<"."<<values[i]->ValueArray->array[0].fieldId;
        for(int j=1;j<values[i]->ValueArray->size;j++)
        {
            std::cout<<", "<<values[i]->ValueArray->array[j].predicateArrayId<<"."<<values[i]->ValueArray->array[j].fieldId;
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

void swap(PredicateOperandArray *operandArray, int a, int b) {
    PredicateOperand tmp = operandArray->array[a];
    operandArray->array[a].fieldId = operandArray->array[b].fieldId;
    operandArray->array[a].predicateArrayId = operandArray->array[b].predicateArrayId;
    operandArray->array[b].fieldId = tmp.fieldId;
    operandArray->array[b].predicateArrayId = tmp.predicateArrayId;
}

int nextIndex = 0;

void getAllPermutations(int size, int permutationSize, PredicateOperandArray* elements, PredicateOperandArray* resultArray) {
    // if size becomes 1 then prints the obtained 
    // permutation 
    if (size == 1) 
    { 
        // printArr(a, n); 
        std::cout<<"index: "<<nextIndex<<std::endl;
        for (int j = 0; j < permutationSize; j++) {
            std::cout<<elements->array[j].predicateArrayId<<"."<<elements->array[j].fieldId<<" ";
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

int getCombinatiosNum(int size, int combinationSize) {
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
void getCombinations(PredicateOperandArray* elements, int n, int r, int index, PredicateOperandArray& data,
                     PredicateOperandArray* resultArray, int i) 
{ 
    // Current cobination is ready, print it 
    if (index == r) { 
        // for (int j = 0; j < r; j++) 
        //     printf("%d ", data[j]); 
        // printf("\n"); 
            // std::cout<<"1a nextIndex: "<<nextIndex<<std::endl;

        resultArray[nextIndex++].init(&data, r);
            // std::cout<<"1b"<<std::endl;

        // for (int i = 0; i < nextIndex; i ++) {
        //     // std::cout<<"i: "<<i<<": ";
        //     for (int k = 0; k < resultArray[i].size; k++) {
        //         std::cout<<resultArray[i].array[k].predicateArrayId<<"."<<resultArray[i].array[k].fieldId<<" ";
        //     }
        //     std::cout<<std::endl;
        // }
        return; 
    } 
  
    // When no more elements are there to put in data[] 
    if (i >= n) 
        return; 
  
    // current is included, put next at next location 
    // data[index] = arr[i]; 
    // std::cout<<"1"<<std::endl;
    data.array[index].fieldId = elements->array[i].fieldId;
        // std::cout<<"2"<<std::endl;

    data.array[index].predicateArrayId = elements->array[i].predicateArrayId;
            // std::cout<<"3"<<std::endl;

    getCombinations(elements, n, r, index + 1, data, resultArray, i + 1); 
  
    // current is excluded, replace it with next 
    // (Note that i+1 is passed, but index is not 
    // changed) 
    getCombinations(elements, n, r, index, data, resultArray, i + 1); 
}

uint64_t** BestPredicateOrder(uint64_t** currentpreds,int cntr,int relationsum,int*relationids,InputArray** inputarr )
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

    PredicateOperandArray predicateOperandArray(4);
    // predicateOperandArray.array = new PredicateOperand[4];
    // predicateOperandArray.size = 4;
    // for (int i = 0; i < relationsum; i++) {
        predicateOperandArray.array[0].predicateArrayId = 3;
        predicateOperandArray.array[0].fieldId = 0;
        predicateOperandArray.array[1].predicateArrayId = 2;
        predicateOperandArray.array[1].fieldId = 1;
        predicateOperandArray.array[2].predicateArrayId = 0;
        predicateOperandArray.array[2].fieldId = 2;
        predicateOperandArray.array[3].predicateArrayId = 4;
        predicateOperandArray.array[3].fieldId = 3;
    // }
    

    for (int i = 1; i < 4; i++) {
        nextIndex = 0;
        int curCombinationsNum = getCombinatiosNum(4, i);
        // std::cout<<curCombinationsNum<<std::endl;
        PredicateOperandArray* resultArray = new PredicateOperandArray[curCombinationsNum];
        // for (int j = 0; j < curCombinationsNum; j++)
        //     resultArray[j].init(NULL, i);

        PredicateOperandArray tempPredicateOperandArray(i);
        // tempPredicateOperandArray.size = i;
        // tempPredicateOperandArray.array = new PredicateOperand[i];
        getCombinations(&predicateOperandArray, predicateOperandArray.size, i, 0, tempPredicateOperandArray, resultArray, 0);
        // delete[] tempPredicateOperandArray.array;
        // std::cout<<"nextindex: "<<nextIndex<<std::endl;
        
        for (int j = 0; j < curCombinationsNum; j++) {
            std::cout<<"j: "<<j<<": ";
            PredicateOperandArray* curArray = &resultArray[j];
            for (int k = 0; k < curArray->size; k++) {
                std::cout<<curArray->array[k].predicateArrayId<<"."<<curArray->array[k].fieldId<<" ";
            }
            std::cout<<std::endl;
        }
        // delete[] resultArray;
    }
// std::cout<<getPermutationsNum(4)<<std::endl;
    

    // std::cout<<predicateOperandArray.array[0].predicateArrayId<<std::endl;
    // std::cout<<predicateOperandArray.array[0].fieldId<<std::endl;
    // swap(&predicateOperandArray, 0, 1);
    //     std::cout<<predicateOperandArray.array[0].predicateArrayId<<std::endl;
    //     std::cout<<predicateOperandArray.array[0].fieldId<<std::endl;

}

