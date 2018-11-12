#include <stdio.h>
#include <stdlib.h>
#include "DataParse.h"
#include "Tables.h"
#include "Results.h"


int main() {

    //table* t = loadRelation("r1");

    table** t = createTablesArray();

    int relr_size = RELR_SIZE;
    int rels_size = RELS_SIZE;

    relation* relation_R = get_relation((char *) relR_name, relr_size);  // Gets the relation from the file DataRelationR.txt
    relation* relation_S = get_relation((char *) relS_name, rels_size);  // Gets the relation from the file DataRelationS.txt

    result* results = RadixHashJoin(relation_R, relation_S);

    freeTable(t);
    freeResults(results);
    free(relation_R->tuples);
    free(relation_R);
    free(relation_S->tuples);
    free(relation_S);

    return 0;
}
