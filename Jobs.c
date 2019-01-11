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

		//add local histogram and psum amounts to the global ones
		mtx_lock(argv->hist_mtx);
		for (int i = 0; i < argv->hash1_value; i++)
		{
			if(localhist[i] != 0)
				argv->histogram[r][i] += localhist[i];
		}
		mtx_unlock(argv->hist_mtx);

		if(r == 0){
			js->thread_histograms_R[argv->threadID] = localhist;
		}
		else{
			js->thread_histograms_S[argv->threadID] = localhist;
		}

	}
}

/**
 * Copies the values of the bucket asigned to the thread into their proper position in the new relation
 */
void PartitionJob(JobScheduler* js, JobQueueElem *argv)
{
	for (int r = 0; r < 2; r++)
	{//for each relation (R and S)

		for(int indx=argv->start[r]; indx<argv->end[r]; indx++){

			int bucket_index = (argv->rels[r]->tuples[indx].payload) % (argv->hash1_value);	// Find the bucket by hashing the value.

			int tuple_index = js->thread_psums[r][argv->threadID][bucket_index];	// Find the position of the tuple in the new relation from the psum table.

			argv->newrels[r]->tuples[tuple_index] = argv->rels[r]->tuples[indx];	// Copy the value into the new relation.

			js->thread_psums[r][argv->threadID][bucket_index]++;		// Increment the psum value.
		}


	}
}

void JoinJob(JobQueueElem *argv)
{
	//WAIT UNTIL BOTH NEW RELATIONS ARE FULLY CREATED

	fprintf(stderr, "JoinJob(): Under Construction\n");

/*	if (argv==NULL)
	{
		fprintf(stderr, "JoinJob(): NULL arguments\n");
		return;
	}

	relation *x = NULL;
    int *x_histogram = NULL;
    int *x_psum = NULL;

    relation *y = NULL;
    int *y_histogram = NULL;
    int *y_psum = NULL;

	int column_id;
	if (histogram[0][argv->bucket_id] > histogram[1][argv->bucket_id])
    {//bucket i of relation S is smaller; S will be hashed
        x = argv->newrels[0];
        x_histogram = argv->histogram[0];
        x_psum = argv->psum[0];

        y = argv->newrels[1];
        y_histogram = argv->histogram[1];
        y_psum = argv->psum[1];

        column_id = 1;
    }
    else
    {//bucket i of relation R is smaller; R will be hashed
        y = argv->newrels[1];
        y_histogram = argv->histogram[0];
        y_psum = argv->psum[0];

        x = argv->newrels[1];
        x_histogram = argv->histogram[1];
        x_psum = argv->psum[1];

        column_id = 2;
    }

    if(y_histogram[argv->bucket_id] == 0)
        continue;


    //create local resultsWithNum
    resultsWithNum* localres = malloc(sizeof(resultsWithNum));
*/
}