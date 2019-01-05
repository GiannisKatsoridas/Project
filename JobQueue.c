#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>

#include "JobQueue.h"




JobQueueElem * JobCreate(int JobID, relation *rels[2], int hash1_value, int start[2], int end[2], 
                        int *hist[2], int *psum[2], sem_t *hist_mtx, 
                        relation *newrels[2],
                        int bucket_id, resultsWithNum *res, sem_t *res_mtx)
{
	JobQueueElem *elem = malloc(sizeof(JobQueueElem));

	elem -> JobID = JobID;
	elem -> rels[0] = rels[0];
	elem -> rels[1] = rels[1];
	elem -> hash1_value = hash1_value;

	elem -> start[0] = start[0];
	elem -> end[0] = end[0];
	elem -> start[1] = start[1];
	elem -> end[1] = end[1];

	elem -> histogram[0] = hist[0];
	elem -> psum[0] = psum[0];
	elem -> histogram[1] = hist[1];
	elem -> psum[1] = psum[1];
	elem -> hist_mtx = hist_mtx;

	elem -> newrels[0] = newrels[0];
	elem -> newrels[1] = newrels[1];

	elem -> bucket_id = bucket_id;
	elem -> res = res;
	elem -> res_mtx = res_mtx;

	return elem;
}


void JobQueueInit(JobQueue** qaddr, int size)
{
	if ((qaddr==NULL)||(size<=0))
	{
		perror("JobQueueInit(): invalid argument");
		return;
	}

	(*qaddr) = malloc(sizeof(JobQueue));
	JobQueue *q = (*qaddr);

	q-> size = size;

	q-> JobArray = malloc((q-> size) * sizeof(JobQueueElem*));
	for (int i = 0; i < q-> size; i++)
	{
		q-> JobArray[i] = NULL;
	}

	semInit(q-> mtx, 1);
	semInit(q-> full, 0);
	semInit(q-> empty, q->size);

	q->counter = 0;
	q->in = 0;
	q->out = 0;
}

void JobQueuePush(JobQueue* q, JobQueueElem *elem)
{//used by JobScheduler (main thread) to push jobs in the queue
	if ((q==NULL) || (elem==NULL))
	{
		perror("JobQueuePush(): NULL argument");
		return;
	}

	P(q->empty);
	P(q->mtx);

	q->JobArray[q->in] = elem;
	q->in = (q->in + 1)% q->size;
	q->counter++;

	V(q->mtx);
	V(q->full);
}

JobQueueElem *JobQueuePop(JobQueue* q)
{//used by thread to take a job from the queue
	if (q==NULL)
	{
		perror("JobQueuePop(): NULL argument");
		return NULL;
	}

	JobQueueElem *elem = NULL;

	P(q->full);
	P(q->mtx);

	elem = q->JobArray[q->out];
	q->JobArray[q->out] = NULL;
	q->out = (q->out + 1)% q->size;
	q->counter--;

	V(q->mtx);
	V(q->empty);

	if(elem==NULL)
		fprintf(stderr, "WARNING: JobQueuePop(): Returning NULL element! \n");

	return elem;
}

void JobQueueDestroy(JobQueue** qaddr)
{
	if (qaddr==NULL)
	{
		perror("(): NULL argument");
		return;
	}

	JobQueue *q = (*qaddr);
	free(q->JobArray);

	semDestroy(q->mtx);
	semDestroy(q->full);
	semDestroy(q->empty);

	free(q);

	(*qaddr) = NULL;
}








void semInit(sem_t *sem, int value)
{
	if ((sem_init(sem, 0, value)) == -1)
	{
		perror("sem_init");
		exit(-1);
	}
}

void P(sem_t *sem)
{
	if((sem_wait(sem))==-1)
	{
		perror("sem_wait()");
		exit(-1);
	}
}

void V(sem_t *sem)
{
	if((sem_post(sem))==-1)
	{
		perror("sem_post()");
		exit(-1);
	}
}

void semDestroy(sem_t* sem)
{
	if((sem_destroy(sem))==-1)
	{
		perror("sem_destroy()");
		exit(-1);
	}
}