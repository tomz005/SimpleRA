#include "global.h"

/**
 * @brief 
 * SYNTAX: EXPORT <relation_name> 
 */

bool syntacticParseEXPORT()
{
    logger.log("syntacticParseEXPORT");
    if (tokenizedQuery.size() == 2)
    {
        parsedQuery.queryType = EXPORT;
        parsedQuery.exportRelationName = tokenizedQuery[1];
        return true;
    }
    else if (tokenizedQuery.size() == 3)
    {
        if (tokenizedQuery[1] != "MATRIX")
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        parsedQuery.queryType = EXPORT;
        parsedQuery.exportMatrixName = tokenizedQuery[2];
        return true;
    }
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
}

bool semanticParseEXPORT()
{
    logger.log("semanticParseEXPORT");
    //Table should exist
    if (tokenizedQuery.size() == 2)
    {
        if (tableCatalogue.isTable(parsedQuery.exportRelationName))
            return true;
    }
    else
    {
        if (matrixCatalogue.isMatrix(parsedQuery.exportMatrixName))
            return true;
    }
    cout << "SEMANTIC ERROR: No such relation/matrix exists" << endl;
    return false;
}

void executeEXPORT()
{
    logger.log("executeEXPORT");
    if (tokenizedQuery.size() == 2)
    {
        Table *table = tableCatalogue.getTable(parsedQuery.exportRelationName);
        table->makePermanent();
    }
    else
    {
        Matrix *matrix = matrixCatalogue.getMatrix(parsedQuery.exportMatrixName);
        matrix->makePermanent();
    }

    return;
}