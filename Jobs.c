#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>

#include "Jobs.h"



void HistogramJob(JobQueueElem *argv)
{
	for (int r = 0; r < 2; r++)
	{//for each relation (R and S)

		//create a local histogram and initialize it
		int localhist[argv -> hash1_value];
		for (int i = 0; i < (argv -> hash1_value); i++)
			localhist[i] = 0;

		//create a local psum
		int localpsum[argv -> hash1_value];
		localpsum[0] = 0;

		//hash values in relation and save their amount to local histogram
		for (int indx = argv->start[r]; indx < argv->end[r]; indx++)
			localhist[(argv->rels[r]->tuples[indx].payload) % (argv->hash1_value)]++;

		//construct local psum from local histogram
		for (int i = 0; i < (argv->hash1_value -1); i++)
			localpsum[i+1] = localpsum[i] + localhist[i];

		//add local histogram and psum amounts to the global ones
		P(argv->hist_mtx);
		for (int i = 0; i < argv->hash1_value; i++)
		{
			if(localhist[i] != 0)
				argv->histogram[r] += localhist[r];
			argv->psum[r] += localpsum[r];
		}
		V(argv->hist_mtx);
	}
}

void PartitionJob(JobQueueElem *argv)
{
	int h = -1;
	for (int r = 0; r < 2; r++)
	{//for each relation (R and S)
		
		//WAIT UNTIL HISTOGRAMS AND PSUMS ARE FULLY CREATED

		for (int indx = argv->start[r]; indx < argv->end[r]; indx++)
		{//for each tuple within my search range in the relation

			//hash the payload of tuple indx and find the position in psum
			h = (argv->rels[r]->tuples[indx].payload) % (argv->hash1_value);

			//copy the tuple from relation[indx] to the new relation, 
			//at the position shown by psum[h] 
			P(argv->hist_mtx);
			argv->newrels[r]->tuples[argv->psum[r][h]] = argv->rels[r]->tuples[indx];
			argv->psum[h]++;
			V(argv->hist_mtx);
		}


	}
}
void JoinJob(JobQueueElem *argv)
{
	//WAIT UNTIL BOTH NEW RELATIONS ARE FULLY CREATED

	fprintf(stderr, "JoinJob(): Under Construction\n");
}