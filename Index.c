#include <stdio.h>
#include <stdlib.h>

#include "Index.h"


int hash2(int32_t payload)
{
    int index =( payload >> RADIX_N ) % HASH2_RANGE;
    //printf("%d -> %d\n", payload, index);
    return index;
}

void index_create(index **indx_addr, int bucket_array_sz)
{
	(*indx_addr) = malloc(sizeof(index));
	index * indx = *indx_addr;

	indx -> bucket_array = malloc(bucket_array_sz * sizeof(int));
	indx -> bucket_array_sz = bucket_array_sz;

	indx -> chain = NULL;
	indx -> chain_sz = -1;
}

void index_fill(index *indx, relation *rel, int tuple_amount, int bucket_end)
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

	//printf("y: bucket[%d]: [%d, %d]\n", i, first_pos, last_pos);
    while(pos >= first_pos)
    {//accessing bucket elements from last to first; insert their position to the index chain 
        //printf("y->pos = %d..\n", pos);
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
    
    /*fprintf(stdout, "\nBUCKET ARRAY\n");
    for (int j = 0; j < HASH2_RANGE; j++)
    {
        fprintf(stdout, "bucket[%02d] = %02d\n", j, bucket_array[j]);
    }
    fprintf(stdout, "\nCHAIN ARRAY\n");
    for (int j = 0; j < y_histogram[i]; j++)
    {
        fprintf(stdout, "chain[%02d] = %02d\n", j, chain[j]);
    }
    fprintf(stdout, "\n");*/
}

void index_destroy(index **indx)
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

int search_val(relation *rel, int bucket_start, index *indx, int32_t key, int32_t payload, int column_id, result* results)
{//search all instances equal to payload in the current index bucket of rel
	//rel is the relation whose bucket is currently indexed
	//bucket_start is the first index of the current bucket in rel
	//the key and payload arguments belong to the other relation and will be compared to the contents of rel
	int results_num = 0;

	int n = hash2(payload);

	if(indx->bucket_array[n] != -1)
	{
		int curr_pos = indx->bucket_array[n];
        int real_pos = curr_pos + bucket_start;
        /*fprintf(stdout, "compairing (x[%d], y[%d]) = (xbucket[%d], ybucket[%d]) = (%d, %d)\n",
        pos, real_pos,
        pos- first_pos, curr_pos,
        x->tuples[pos].payload, y->tuples[real_pos].payload);*/

        if (payload == rel->tuples[real_pos].payload)
        {
            //fprintf(stdout, "equals!\n");
            if(column_id == 1)//relation R tuple (key, payload) is compared to bucket of relation S
            	results_num = add_result(results, key, rel->tuples[real_pos].key);
            else if(column_id==2)//relation S tuple (key, payload) is compared to bucket of relation R
            	results_num = add_result(results, rel->tuples[real_pos].key, key);
        }
        while(indx -> chain[curr_pos] != -1)
        {
        	curr_pos = indx->chain[curr_pos];
        	real_pos = curr_pos + bucket_start;
            /*fprintf(stdout, "compairing (x[%d], y[%d]) = (xbucket[%d], ybucket[%d]) = (%d, %d)\n",
	        pos, real_pos,
	        pos- first_pos, curr_pos,
	        x->tuples[pos].payload, y->tuples[real_pos].payload);*/

	        if (payload == rel->tuples[real_pos].payload)
		    {
		        //fprintf(stdout, "equals!\n");
		        if(column_id == 1)//relation R tuple (key, payload) is compared to bucket of relation S
		        	results_num = add_result(results, key, rel->tuples[real_pos].key);
		        else if(column_id==2)//relation S tuple (key, payload) is compared to bucket of relation R
		        	results_num = add_result(results, rel->tuples[real_pos].key, key);
		    }
        }
	}
	return results_num;
}

/*int search_all_val(relation *x, int* x_hist, int *x_psum, relation *y, int* y_hist, int* y_psum, int bucket_id, index * indx, int column_id, result* results)
{
	//searches all values of the bucket in relation x for equal payloads in the same bucket of relation y
	int results_num = 0;

     //assign boundaries of the bucket i of relation x
    int first_pos = x_psum[i] - x_histogram[i]; //starting position of bucket i within relation x
    int last_pos = x_psum[i] -1;  //ending position of bucket i within relation x
    int pos = last_pos;     //variable for current position
    
    //values of tuple to be searched
    int32_t key;
    int32_t payload;

    //real index of the first element of the bucket bucket_id of relation y 
    int bucket_start;
    if(bucket_id > 0)
    	bucket_start = y_psum[i-1];
    else
    	bucket_start = 0;

    while(pos >= first_pos)
    {
    	key = x->tuples[pos].key;
    	payload = x->tuples[pos].payload;

    	results_num = results_num + search_val(y, bucket_start, indx, key, payload, column_id, results);
    	
    	pos--;
    }
    return results_num;
}*/