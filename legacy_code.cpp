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
