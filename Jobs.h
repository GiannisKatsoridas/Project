#ifndef _JOBS_H_
#define _JOBS_H_

#include "JobQueue.h"

sem_t histogramsSem;

void HistogramJob(JobQueueElem *argv);
void PartitionJob(JobQueueElem *argv);
void JoinJob(JobQueueElem *argv);


#endif