#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include "JobQueue.h"


JobQueueElem * JobCreate(int JobID, int threadID, int jobType, relation *rels[2], int hash1_value, int start[2],
                        int end[2], int *hist[2], int *psum[2], pthread_mutex_t *hist_mtx,
                        relation *newrels[2],
                        int bucket_id, resultsWithNum *res, pthread_mutex_t *res_mtx)
{
	JobQueueElem *elem = malloc(sizeof(JobQueueElem));

	elem -> JobID = JobID;
	elem -> threadID = threadID;
	elem -> jobType = jobType;
	elem -> rels[0] = rels[0];
	elem -> rels[1] = rels[1];
	elem -> hash1_value = hash1_value;

    elem -> start[0] = start[0];
    elem -> end[0] = end[0];
    elem -> start[1] = start[1];
    elem -> end[1] = end[1];

    elem -> psum[0] = psum[0];
    elem -> psum[1] = psum[1];

	if(jobType == 1) {
		elem->histogram[0] = hist[0];
		elem->histogram[1] = hist[1];
		elem->hist_mtx = hist_mtx;
	}
	else if (jobType == 2) {
		elem->newrels[0] = newrels[0];
		elem->newrels[1] = newrels[1];
	}
	else if (jobType == 3) {
		elem->bucket_id = bucket_id;
		elem->res = res;
		elem->res_mtx = res_mtx;
	}

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

	mtx_init(&q->queue_mtx);

	q-> full = malloc(sizeof(sem_t));
	semInit(q-> full, 0);

	q->empty = malloc(sizeof(sem_t));
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

	q->JobArray[q->in] = elem;
	q->in = (q->in + 1)% q->size;
	q->counter++;

	//fprintf(stderr, "JobQueuePush()\n");
}

JobQueueElem *JobQueuePop(JobQueue* q)
{//used by thread to take a job from the queue
	if (q==NULL)
	{
		perror("JobQueuePop(): NULL argument");
		return NULL;
	}

	JobQueueElem *elem = NULL;

	elem = q->JobArray[q->out];
	q->JobArray[q->out] = NULL;
	q->out = (q->out + 1)% q->size;
	q->counter--;

	if(elem==NULL)
		fprintf(stderr, "WARNING: JobQueuePop(): Returning NULL element! \n");

	//fprintf(stderr, "JobQueuePop()\n");
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

	mtx_destroy(&q->queue_mtx);

	semDestroy(q->full);
	free(q->full);
	
	semDestroy(q->empty);
	free(q->empty);

	free(q);

	(*qaddr) = NULL;
}


//////////////////////////////////////////////////////////////////////////
/// PTHREAD MUTEX 

void mtx_init(pthread_mutex_t *mtx)
{
	if ((pthread_mutex_init(mtx, NULL)) != 0)
	{
		perror("pthread_mutex_init()");
		exit(-1);
	}
}

void mtx_lock(pthread_mutex_t *mtx)
{
	if((pthread_mutex_lock(mtx))!= 0)
	{
		perror("pthread_mutex_lock()");
		exit(-1);
	}
}

void mtx_unlock(pthread_mutex_t *mtx)
{
	if((pthread_mutex_unlock(mtx))!= 0)
	{
		perror("pthread_mutex_unlock()");
		exit(-1);
	}
}

void mtx_destroy(pthread_mutex_t* mtx)
{
	if((pthread_mutex_destroy(mtx))!= 0)
	{
		perror("pthread_mutex_destroy()");
		exit(-1);
	}
}




//////////////////////////////////////////////////////////////////////////
///POSIX SEMAPHORES

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