#include <stdio.h>
#include <stdlib.h>

#include "Index.h"


int hash2(int32_t payload)
{
    int n =( payload >> RADIX_N ) % HASH2_RANGE;
    return n;
}

void index_create(hash_index **indx_addr, int bucket_array_sz)
{
    (*indx_addr) = malloc(sizeof(hash_index));
    hash_index * indx = *indx_addr;

    indx -> bucket_array = malloc(bucket_array_sz * sizeof(int));
    indx -> bucket_array_sz = bucket_array_sz;

    indx -> chain = NULL;
    indx -> chain_sz = -1;
}

void index_fill(hash_index *indx, relation *rel, int tuple_amount, int bucket_end)
{//tuple amount: histogram[i] ; bucket_end : psum[i]

    //initialize bucket array
    for (int j = 0; j < indx-> bucket_array_sz; j++)
    {
        indx->bucket_array[j] = -1;
    }

    //now the bucket i of the relation y is the one that will be hashed
    //create the chain array
    indx -> chain = realloc(indx -> chain, tuple_amount * sizeof(int));
    for (int j = 0; j < tuple_amount; j++)
    {
        indx -> chain[j] = -1;
    }
    indx -> chain_sz = tuple_amount;

    //assign boundaries of the bucket i of relation y
    int first_pos = bucket_end - tuple_amount; //starting position of bucket i within relation y
    int last_pos = bucket_end -1;  //ending position of bucket i within relation y
    int pos = last_pos;     //variable for current position

    while(pos >= first_pos)
    {//accessing bucket elements from last to first; insert their position to the index chain 
        int n = hash2(rel->tuples[pos].payload);
        if (indx -> bucket_array[n]== -1)
        {
            indx -> bucket_array[n] = pos - first_pos;
        }
        else
        {
            int temp_pos = indx -> bucket_array[n];
            while(indx -> chain[temp_pos] != -1)
            {
                temp_pos = indx -> chain[temp_pos];
            }
            indx -> chain[temp_pos] = pos - first_pos;
        }
        pos--;
    }

}

void index_destroy(hash_index **indx)
{
    free((*indx) -> bucket_array);
    (*indx) -> bucket_array = NULL;
    (*indx) -> bucket_array_sz = -1;

    free((*indx) -> chain);
    (*indx) -> chain = NULL;
    (*indx) -> chain_sz = -1;

    free(*indx);
    *indx = NULL;
}

void search_val(relation *rel, int bucket_start, hash_index *indx, int32_t key, int32_t payload, int column_id, resultsWithNum* res)
{//search all instances equal to payload in the current index bucket of rel
    //rel is the relation whose bucket is currently indexed
    //bucket_start is the first index of the current bucket in rel
    //the key and payload arguments belong to the other relation and will be compared to the contents of rel

    int n = hash2(payload);

    if(indx->bucket_array[n] != -1)
    {
        int curr_pos = indx->bucket_array[n];
        int real_pos = curr_pos + bucket_start;
        if (payload == rel->tuples[real_pos].payload)
        {
            if(column_id == 1)//relation R tuple (key, payload) is compared to bucket of relation S
                add_result(res, key, rel->tuples[real_pos].key);
            else if(column_id==2)//relation S tuple (key, payload) is compared to bucket of relation R
                add_result(res, rel->tuples[real_pos].key, key);
        }
        while(indx -> chain[curr_pos] != -1)
        {
            curr_pos = indx->chain[curr_pos];
            real_pos = curr_pos + bucket_start;

            if (payload == rel->tuples[real_pos].payload)
            {
                if(column_id == 1)//relation R tuple (key, payload) is compared to bucket of relation S
                    add_result(res, key, rel->tuples[real_pos].key);
                else if(column_id==2)//relation S tuple (key, payload) is compared to bucket of relation R
                    add_result(res, rel->tuples[real_pos].key, key);
            }
        }
    }
}
