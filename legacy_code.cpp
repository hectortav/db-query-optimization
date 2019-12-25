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

relation* re_orderedd(relation *rel, relation* new_rel, int no_used)
{
    int shift = 0;
    uint64_t x = pow(2, 8), array_size = x;
    //create histogram
    uint64_t** hist = create_hist(rel, shift), **temp_hist = NULL;
    //create psum
    uint64_t** psum = create_psum(hist, x);
    uint64_t payload;
    uint64_t i, j, y;
    bool clear;
    uint64_t** arr = new uint64_t*[3];
    for(i = 0; i < 3; i++)
        arr[i] = new uint64_t[8];
        uint64_t** tempPsum = new uint64_t*[3];
    for (uint64_t i = 0; i < 3; i++) {
        tempPsum[i] = new uint64_t[x];
        memcpy(tempPsum[i], psum[i], x*sizeof(uint64_t));
    }
  
    i = 0;
    while(i < rel->num_tuples)
    {
        payload = hashFunction(rel->tuples[i].payload, 7 - shift);
        //find hash in psum = pos in new relation
        new_rel->tuples[tempPsum[1][payload]].payload = rel->tuples[i].payload;
        new_rel->tuples[tempPsum[1][payload]++].key = rel->tuples[i].key;
        i++;
    }

    clear = false; //make a full loop with clear == false to end
    i = 0;
    while (i < array_size)
    {
        if ((hist[1][i] > TUPLES_PER_BUCKET) && (hist[2][i] < 7))
        {
            clear = true;
            //new relation from psum[1][i] to psum[1][i+1]
            if (rel == NULL)
                rel = new relation();
            uint64_t first = psum[1][i];
            uint64_t last = new_rel->num_tuples;
            if (i != array_size - 1)
                last = psum[1][i+1];
            rel->num_tuples = last - first;
            if(rel->tuples == NULL)
                rel->tuples = new tuple[new_rel->num_tuples];
        
            memcpy(rel->tuples, new_rel->tuples + first, rel->num_tuples*sizeof(tuple));
          
            temp_hist = create_hist(rel, hist[2][i] + 1);
           
            hist = combine_hist(hist, temp_hist, i, array_size);
            array_size+=x;

            delete [] psum[0];
            delete [] psum[1];
            delete [] psum[2];
            delete [] psum;
            psum = create_psum(hist, array_size);

            j = 0;
            if (rel == NULL)
                rel = new relation();
            if (sizeof(*rel->tuples) != sizeof(*new_rel->tuples))
            {
                delete [] rel->tuples;
                rel->tuples = new tuple[new_rel->num_tuples];
            }
            rel->num_tuples = new_rel->num_tuples;

            for (uint64_t i = 0; i < 3; i++) {
                delete[] tempPsum[i];
                tempPsum[i] = new uint64_t[array_size];
                memcpy(tempPsum[i], psum[i], array_size*sizeof(uint64_t));
            }
            
            while(j < new_rel->num_tuples)
            {
                //hash
                payload = find_shift(hist, array_size, new_rel->tuples[j].payload, arr);
                //find hash in psum = pos in new relation
                rel->tuples[tempPsum[1][payload]].payload = new_rel->tuples[j].payload;
                rel->tuples[tempPsum[1][payload]++].key = new_rel->tuples[j].key;
                j++;
            }


            tuple *temp_tuple;
            temp_tuple = rel->tuples;
            rel->tuples = new_rel->tuples;
            new_rel->tuples = temp_tuple;
            j = rel->num_tuples;
            rel->num_tuples = new_rel->num_tuples;
            new_rel->num_tuples = j;

            i+=(x-1);   //NOT SURE
        }
      
        if (hist[1][i] <= TUPLES_PER_BUCKET || hist[2][i] > 7)
        {
            if (hist[1][i] > 0)
            {
                if (i + 1 < array_size)
                    sortBucket(new_rel, psum[1][i], psum[1][i+1] - 1);
                else
                    sortBucket(new_rel, psum[1][i], rel->num_tuples - 1);
                new_rel->print();
            }
        }

        i++;
        if (i == array_size && clear)
        {
            i = 0;
            clear = false;
        }
    }
    //testing
    //print_psum_hist(psum, hist, array_size);

    for (uint64_t i = 0; i < 3; i++) {
        delete[] tempPsum[i];
    }
    delete[] tempPsum;
    
    delete [] hist[0];
    delete [] hist[1];
    delete [] hist[2];
    delete [] hist;
    
    delete [] psum[0];
    delete [] psum[1];
    delete [] psum[2];
    delete [] psum;

    delete [] arr[0];
    delete [] arr[1];
    delete [] arr[2];

    delete [] arr;

    return new_rel;
 }


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
