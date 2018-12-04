#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "DataParse.h"
#include "Tables.h"
#include "Results.h"
#include "Query.h"


int main() {

    //table* t = loadRelation("r1");

    table** t = createTablesArray();

    parseTableData(t);

    //printf("Done\n");

    printf("%d relations.\n", relationsNum);
    for (int i = 0; i < relationsNum; i++)
    {
        printf("%lu X %lu. ", t[i]->size, t[i]->columns_size);
        printf("Min: %lu, Max: %lu, Distinct values: %d\n", 
            t[i]->metadata->min,
            t[i]->metadata->max,
            t[i]->metadata->distincts);
    }

    freopen("queries.txt","r",stdin);

    Query* queries = getQueries();

    while(queries != NULL){
        //printf("%d - %d - %d\n", queries[0].comparisons->comparisons[0].relationA, queries[1].columns->columns[1].relation, queries[0].comparisons->comparisons[1].columnB);
        freeQueries(queries);
        queries = getQueries();
    }


    int relr_size = RELR_SIZE;
    int rels_size = RELS_SIZE;


    /*relation* relation_R = get_relation((char *) relR_name, relr_size);  // Gets the relation from the file DataRelationR.txt
    relation* relation_S = get_relation((char *) relS_name, rels_size);  // Gets the relation from the file DataRelationS.txt

    result* results = RadixHashJoin(relation_R, relation_S);

    printf("%d\n", getResultsAmount());

    freeResults(results);
    free(relation_R->tuples);
    free(relation_R);
    free(relation_S->tuples);
    free(relation_S);*/

    freeTable(t);
    freeQueries(queries);

    return 0;
}
