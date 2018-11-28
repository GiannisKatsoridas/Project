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

    printf("Done\n");

    printf("%d - %d\n", (int) t[1]->columns[1][3], (int) t[0]->columns[2][30]);

    Query* queries = getQueries();

    printf("%d - %d - %d\n", queries[0].comparisons->comparisons[0].relationA, queries[1].columns->columns[1].relation, queries[0].comparisons->comparisons[1].columnB);

    int relr_size = RELR_SIZE;
    int rels_size = RELS_SIZE;


/*    relation* relation_R = get_relation((char *) relR_name, relr_size);  // Gets the relation from the file DataRelationR.txt
    relation* relation_S = get_relation((char *) relS_name, rels_size);  // Gets the relation from the file DataRelationS.txt

    result* results = RadixHashJoin(relation_R, relation_S);

    freeResults(results);
    free(relation_R->tuples);
    free(relation_R);
    free(relation_S->tuples);
    free(relation_S);*/
    freeTable(t);
    freeQueries(queries);

    return 0;
}
