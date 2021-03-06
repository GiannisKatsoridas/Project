#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <string.h>
#include <unistd.h>

#include "Tables.h"

table* loadRelation(const char* fileName){

    table* t = malloc(sizeof(table));

    int fd = open(fileName, O_RDONLY);
    if (fd==-1) {
        printf("ERROR. Cannot open %s\n", fileName);
        return NULL;
    }

    // Obtain file size
    struct stat sb;
    if (fstat(fd,&sb)==-1)
        printf("Pstat Error.\n");

    int length= (int) sb.st_size;

    char* addr= (char*) (mmap(NULL,length,PROT_READ,MAP_PRIVATE,fd,0u));
    if (addr==MAP_FAILED) {
        printf("Cannot MMAP. ERROR.\n");
    }

    if (length<16) {
        printf("relation file %s does not contain a valid header.\n", fileName);
    }

    t->size = *(uint64_t*)(addr);
    addr += sizeof(t->size);

    size_t numColumns = *(size_t*)(addr);
    t->columns_size = numColumns;
    addr += sizeof(size_t);

    t->columns = malloc(numColumns*sizeof(uint32_t*));

    for (unsigned i=0 ; i<numColumns ; ++i) {
        t->columns[i] = (uint64_t*) (addr);
        addr += t->size*sizeof(uint64_t);
    }

    //close(fd);
    return t;
}

table **createTablesArray() {

    relationsNum = 0;
    int i;
    table **t = NULL;
    table **t1 = NULL;

    char* file = NULL;
    size_t length;
    size_t size;

    size = (size_t) getline(&file, &length, stdin);

    while((strncmp(file, "Done", 4)!=0)&&((int) size > 0)){
        if(file[strlen(file)-1]=='\n'){
            file[strlen(file) - 1] = '\0';
        }

        relationsNum++;

        t1 = malloc(relationsNum*sizeof(table*));

        for(i=0; i<relationsNum-1; i++){
            t1[i] = t[i];
        }

        t1[relationsNum-1] = loadRelation(file);

        free(t);

        t = t1;

        free(file);
        file = NULL;

        size = (size_t) getline(&file, &length, stdin);
    }

    free(file);

    return t;
}

void parseTableData(table** t){

    for(int i=0; i<relationsNum; i++){

        t[i]->metadata = malloc(t[i]->columns_size*sizeof(Metadata));

        for(int j=0; j<t[i]->columns_size; j++){

            findMinMax(t, i, j);

            u_int64_t range = t[i]->metadata[j].max - t[i]->metadata[j].min + 1;

            if(range > MAX_TABLE_RANGE){
                t[i]->metadata[j].distincts = -1;
                continue;
            }

            findAllDistincts(t, i, j);

            t[i]->metadata[j].size = (int) t[i]->size;

        }

        t[i]->tableID = i;

    }

}

void findMinMax(table** t, int i, int j){

    t[i]->metadata[j].max = t[i]->columns[j][0];
    t[i]->metadata[j].min = t[i]->columns[j][0];

    for(int k=0; k<t[i]->size; k++){

        if(t[i]->columns[j][k] < t[i]->metadata[j].min){
            t[i]->metadata[j].min = t[i]->columns[j][k];
        }
        else if(t[i]->columns[j][k] > t[i]->metadata[j].max){
            t[i]->metadata[j].max = t[i]->columns[j][k];
        }

    }

}

void findAllDistincts(table** t, int i, int j){

    u_int64_t range = t[i]->metadata[j].max - t[i]->metadata[j].min + 1;

    if(range > MAX_TABLE_RANGE)
        range = MAX_TABLE_RANGE;

    char* values = malloc(sizeof(char)*range);
    for(int k=0; k<range; k++){
        values[k] = 0;
    }

    for(int k=0; k<t[i]->size; k++){
        if(values[(t[i]->columns[j][k] - t[i]->metadata[j].min) % range] == 0)
            values[(t[i]->columns[j][k] - t[i]->metadata[j].min) % range] = 1;
    }

    t[i]->metadata[j].distincts = 0;

    for(int k=0; k<range; k++){
        if(values[k] == 1)
            t[i]->metadata[j].distincts++;
    }

    free(values);
}

void freeTable(table** t){

    for(int i=0; i<relationsNum; i++){

        free(t[i]->columns);
        free(t[i]->metadata);
        free(t[i]);
    }

    free(t);
}

