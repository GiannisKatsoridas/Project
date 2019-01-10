#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Actions.h"

int main() {

    setTuplesPerPage();
    //table* t = loadRelation("r1");

    freopen("files_and_queries.txt", "r", stdin);

    table** t = createTablesArray();

    parseTableData(t);

    //printf("Done\n");

    //freopen("queries.txt","r",stdin);

    Query* queries = getQueries();
    int queries_num;
    while(queries != NULL){
        //fprintf(stderr, "Reading batch...\n");
        queries_num = getQueriesNum();
        for(int k=0; k<queries_num; k++) {
            executeQuery(t, &(queries[k]));
        }

        freeQueries(queries);
        //fprintf(stderr, "Ready to read another batch...\n");
        queries = getQueries();
    }


    freeTable(t);
    freeQueries(queries);

    return 0;
}
