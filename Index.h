#ifndef _INDEX_H_
#define _INDEX_H_

#include "Results.h"


#define HASH2_RANGE 10

int hash2(int32_t payload);


struct index
{
	//int bucket_id;

	int *bucket_array;
	int bucket_array_sz;

	int *chain;
	int chain_sz;

	//int *curr_elem;
};

typedef struct index index;

int hash2(int32_t payload);

void index_create(index **indx_addr, int bucket_array_sz);

void index_fill(index *indx, relation *rel, int tuple_amount, int bucket_end);

void index_destroy(index **indx);

int search_val(relation *rel, int bucket_start, index *indx, int32_t key, int32_t payload, int column_id, result* results);

//int search_all_val(relation *x, int* x_hist, int *x_psum, relation *y, int* y_hist, int* y_psum, int bucket_id, index * indx, int column_id, result* results);


#endif