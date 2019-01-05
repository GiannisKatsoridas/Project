#ifndef _JOB_QUEUE_H_
#define _JOB_QUEUE_H_

#include "Globals.h"

typedef struct{
    int JobID;
    relation *rels[2];//relations R and S

    int hash1_value;//number of buckets
    int start[2];   //current starting access index in relations R and S
    int end[2];     //current ending access index in relations R and S

    //HistogramJob
    int *histogram[2];//histograms for relations R and S
    int *psum[2];//psums for relations R and S
    sem_t *hist_mtx;

    //PartitionJob
    relation *newrels[2];

    //JoinJob
    int bucket_id;

    resultsWithNum *res;
    sem_t *res_mtx;
}JobQueueElem;


typedef struct{
	JobQueueElem **JobArray;
    int size;

	sem_t *mtx;    //mutex semaphore for accessing JobArray
    sem_t *full;    //counting semaphore showing produced items in the buffer (initialized to 0)
	sem_t *empty;     //counting semaphore showing remaining space in the buffer (initialized to size)
	
	int counter;   //element counter within the array
	int in;        //index of next input in the array
	int out;       //points to the element that will be popped next
}JobQueue;



JobQueueElem * JobCreate(int JobID, relation *rels[2], int hash1_value, int start[2], int end[2], 
                        int *hist[2], int *psum[2], sem_t *hist_mtx, 
                        relation *newrels[2],
                        int bucket_id, resultsWithNum *res, sem_t *res_mtx);


void JobQueueInit(JobQueue** qaddr, int size);

//used by JobScheduler (main thread) to push jobs in the queue
void JobQueuePush(JobQueue* q, JobQueueElem *elem);

//used by thread to take a job from the queue
JobQueueElem *JobQueuePop(JobQueue* q);


void JobQueueDestroy(JobQueue** qaddr);




void semInit(sem_t *sem, int value);
void P(sem_t *sem);
void V(sem_t *sem);
void semDestroy(sem_t* sem);

#endif