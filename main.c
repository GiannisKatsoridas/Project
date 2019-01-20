#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Actions.h"

int main() {

    setTuplesPerPage();

    freopen("files_and_queries.txt", "r", stdin);

    table** t = createTablesArray();

    parseTableData(t);

    Query* queries = getQueries();
    int queries_num;
    while(queries != NULL){
        queries_num = getQueriesNum();
        for(int k=0; k<queries_num; k++) {
            executeQuery(t, &(queries[k]));
        }

        freeQueries(queries);
        queries = getQueries();
    }


    freeTable(t);
    freeQueries(queries);

    return 0;
}
