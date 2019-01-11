#ifndef _JOBS_H_
#define _JOBS_H_

#include "JobQueue.h"
#include "JobScheduler.h"


void HistogramJob(JobScheduler* js, JobQueueElem *argv);
void PartitionJob(JobScheduler* js, JobQueueElem *argv);
void JoinJob(JobQueueElem *argv);


#endif