#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include <pthread.h>
#include <string.h>

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
		if(r == 0)
			js->thread_histograms_R[argv->threadID] = localhist;
		else
			js->thread_histograms_S[argv->threadID] = localhist;

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
    int *y_histogram = NULL;
    int *y_psum = NULL;

    int column_id;// == 1 if tuples(bucket(R)) > tuples(bucket(S)), == 2 otherwise

    //declare temporary position variables
    int pos = -1;
    int first_pos = -1;
    int last_pos = -1;

    //compare size of bucket i of the relations R and S
    if (argv->histogram[R][argv->bucket_id] > argv->histogram[S][argv->bucket_id])
    {//bucket i of relation S is smaller; S will be hashed
        x = argv->newrels[R];

        first_pos = argv->start[R];
        last_pos = argv->end[R];
        pos = last_pos;

        y = argv->newrels[S];
        y_histogram = argv->histogram[S];
        y_psum = argv->psum[S];

        column_id = 1;
    }
    else
    {//bucket i of relation R is smaller; R will be hashed
        y = argv->newrels[R];
        y_histogram = argv->histogram[R];
        y_psum = argv->psum[R];

        first_pos = argv->start[S];
        last_pos = argv->end[S];
        pos = last_pos;

        x = argv->newrels[S];

        column_id = 2;
    }
    if(y_histogram[argv->bucket_id] == 0)
    {
        index_destroy(&indx);
        return;
    }

    //hash that bucket into the index
    index_fill(indx, y, y_histogram[argv->bucket_id], y_psum[argv->bucket_id]);

    int bucket_start;
    if(argv->bucket_id>0)
        bucket_start = y_psum[argv->bucket_id-1];
    else
        bucket_start = 0;

    //values of tuple to be searched
    int32_t key;
    int32_t payload;

    //local result list
    resultsWithNum* localres = create_resultsWithNum();

    while(pos >= first_pos)
    {//for each value of the greater bucket

        key = x->tuples[pos].key;
        payload = x->tuples[pos].payload;

        //do a search_val: search for in in the index and save it in a local results list
        search_val(y, bucket_start, indx, key, payload, column_id, localres);

        pos--;
    }

    //concatenate local result list to the total result list
    mtx_lock(argv->res_mtx);
    concatResults(argv->res, localres);
    mtx_unlock(argv->res_mtx);

    //destroy index
    index_destroy(&indx);
}