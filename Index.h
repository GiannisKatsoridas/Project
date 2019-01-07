#ifndef _INDEX_H_
#define _INDEX_H_

#include "Results.h"


#define HASH2_RANGE 10

//2nd hash function
int hash2(int32_t payload);


struct hash_index
{
	int *bucket_array;
	int bucket_array_sz;

	int *chain;
	int chain_sz;
};
typedef struct hash_index hash_index;

//create and initialize index; allocate memory for buffer array
void index_create(hash_index **indx_addr, int bucket_array_sz);

//create index for bucket of relation rel; bucket contains <tuple_amount> tuples and its last element is pointed by <bucket_end> -1 in rel
void index_fill(hash_index *indx, relation *rel, int tuple_amount, int bucket_end);

//free memory allocated for index
void index_destroy(hash_index **indx);

//given a tuple <(key, payload)>, search <payload> in the bucket pointed by <indx> of <rel>, and if equal payloads found, add <(key1, key2)> to <results>  
void search_val(relation *rel, int bucket_start, hash_index *indx, int32_t key, int32_t payload, int column_id, resultsWithNum* results);

//int search_all_val(relation *x, int* x_hist, int *x_psum, relation *y, int* y_hist, int* y_psum, int bucket_id, index * indx, int column_id, result* results);


#endif