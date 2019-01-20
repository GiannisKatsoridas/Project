#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include <pthread.h>

#include "Jobs.h"
#include "Index.h"
#include "JobScheduler.h"



void HistogramJob(JobScheduler* js, JobQueueElem *argv)
{
	for (int r = 0; r < 2; r++)
	{//for each relation (R and S)

		//create a local histogram and initialize it
		int *localhist = malloc(argv->hash1_value * sizeof(int));
		for (int i = 0; i < (argv -> hash1_value); i++)
			localhist[i] = 0;

		//hash values in relation and save their amount to local histogram
		for (int indx = argv->start[r]; indx < argv->end[r]; indx++)
			localhist[(argv->rels[r]->tuples[indx].payload) % (argv->hash1_value)]++;

		//add local histogram amounts to the global ones
		mtx_lock(argv->hist_mtx);
		for (int i = 0; i < argv->hash1_value; i++)
		{
			if(localhist[i] != 0)
				argv->histogram[r][i] += localhist[i];
		}
		mtx_unlock(argv->hist_mtx);

		//save local histogram
		if(r == 0){
			js->thread_histograms_R[argv->threadID] = localhist;
		}
		else{
			js->thread_histograms_S[argv->threadID] = localhist;
		}

	}
}

/**
 * Copies the values of the bucket assigned to the thread into their proper position in the new relation
 */
void PartitionJob(JobScheduler* js, JobQueueElem *argv)
{
	for (int r = 0; r < 2; r++)
	{//for each relation (R and S)

		for(int indx=argv->start[r]; indx<argv->end[r]; indx++){

			// Find the bucket by hashing the value.
			int bucket_index = (argv->rels[r]->tuples[indx].payload) % (argv->hash1_value);	

			// Find the position of the tuple in the new relation from the psum table.
			int tuple_index = js->thread_psums[r][argv->threadID][bucket_index];	

			// Copy the value into the new relation.
			argv->newrels[r]->tuples[tuple_index] = argv->rels[r]->tuples[indx];	

			// Increment the psum value.
			js->thread_psums[r][argv->threadID][bucket_index]++;		
		}

	}
}

void JoinJob(JobQueueElem *argv)
{
	//create an index with a bucket_array[#of tuples in bucket] and a chain[rash2_range]
	hash_index *indx = NULL;
    index_create(&indx, HASH2_RANGE);

	//find which one of the new relations' bucket is smaller
	
	//create 2 temporary sets of relations, histograms and psums: x and y
    //y will be the relation whose bucket will be the smallest (and consequently indexed)
    //x will be the relation whose bucket will be the biggest

    relation *x = NULL;

    relation *y = NULL;

    int column_id;// == 1 if tuples(bucket(R)) > tuples(bucket(S)), == 2 otherwise

    //declare temporary position variables
    int x_curr= -1;
    int x_start = -1;
    int x_end = -1;
    int x_amount = -1;

    int y_start = -1;
    int y_end = -1;
    int y_amount = -1;


 	//compare size of bucket i of the relations R and S
    if (argv->histogram[R][argv->bucket_id] > argv->histogram[S][argv->bucket_id])
    {//bucket i of relation S is smaller; S will be hashed
        x = argv->newrels[R];

        x_start = argv->start[R];
        x_end = argv->end[R];
        x_curr = x_end;
        x_amount = argv->histogram[R][argv->bucket_id];


        y = argv->newrels[S];

        y_start = argv->start[S];
        y_end = argv->end[S];
        y_amount = argv->histogram[S][argv->bucket_id];

        column_id = 1;
    }
    else
    {//bucket i of relation R is smaller; R will be hashed
        y = argv->newrels[R];

        y_start = argv->start[R];
        y_end = argv->end[R];
        y_amount = argv->histogram[R][argv->bucket_id];

        
        x = argv->newrels[S];

        x_start = argv->start[S];
        x_end = argv->end[S];
        x_curr= x_end;
        x_amount = argv->histogram[S][argv->bucket_id];

        column_id = 2;
    }
    //fprintf(stderr, "BUCKET #%d\n",i);
    if(y_start == y_end)
    {
    	index_destroy(&indx);
        return;
    }

    mtx_lock(argv->res_mtx);

   /* if(column_id == 1)
    {
    	printf("bucket = %02d - bucket(R) >> bucket(S) - ", argv->bucket_id);
    	printf("bucket(R) -> [%5d, %5d]  (range: %5d) - num_tuples(R): %5d\t - ", 	x_start, 	x_end, 		x_amount, 	argv->rels[R]->num_tuples);
    	printf("bucket(S) -> [%5d, %5d]  (range: %5d) - num_tuples(S): %5d\n", 		y_start,	y_end,		y_amount, 	argv->rels[S]->num_tuples);
    }
    else
    {
    	printf("bucket = %02d - bucket(R) << bucket(S) - ", argv->bucket_id);
    	printf("bucket(S) -> [%5d, %5d]  (range: %5d) - num_tuples(R): %5d\t - ", 	x_start, 	x_end, 		x_amount, 	argv->rels[S]->num_tuples);
    	printf("bucket(R) -> [%5d, %5d]  (range: %5d) - num_tuples(S): %5d\n", 		y_start, 	y_end, 		y_amount,	argv->rels[R]->num_tuples);    
    }*/

	mtx_unlock(argv->res_mtx);

	//hash that bucket into the index
    index_fill(indx, y, y_amount, y_end);

    //assign boundaries of the bucket of relation x
    //x_start = x_psum[argv->bucket_id] - x_histogram[argv->bucket_id]; //starting position of bucket i within relation x
    //x_end = x_psum[argv->bucket_id] -1;  //ending position of bucket i within relation x
    //x_curr= x_end;     //variable for current position

    //values of tuple to be searched
    int32_t key;
    int32_t payload;

    //local result list
    resultsWithNum* localres = create_resultsWithNum();

	while(x_curr>= x_start)
	{//for each value of the greater bucket

	    key = x->tuples[x_curr].key;
	    payload = x->tuples[x_curr].payload;

		//do a search_val: search for in in the index and save it in a local results list
	    search_val(y, y_start, indx, key, payload, column_id, localres);
	    
	    x_curr--;
	}

	//concatenate local result list to the total result list
	mtx_lock(argv->res_mtx);
	concatResults(argv->res, localres);
	mtx_unlock(argv->res_mtx);

	//destroy index
	index_destroy(&indx);
}