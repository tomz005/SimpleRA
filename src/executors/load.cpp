#include "global.h"
/**
 * @brief 
 * SYNTAX: LOAD relation_name
 */
bool syntacticParseLOAD()
{
    logger.log("syntacticParseLOAD");
    if (tokenizedQuery.size() == 2)
    {
        parsedQuery.queryType = LOAD;
        parsedQuery.loadRelationName = tokenizedQuery[1];
        return true;
    }
    else if (tokenizedQuery.size() == 3)
    {
        if (tokenizedQuery[1] != "MATRIX")
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        else
        {
            parsedQuery.queryType = LOAD;
            parsedQuery.loadMatrixName = tokenizedQuery[2];
        }
    }
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
}

bool semanticParseLOAD()
{
    logger.log("semanticParseLOAD");
    if (tokenizedQuery.size() == 2)
    {
        if (tableCatalogue.isTable(parsedQuery.loadRelationName))
        {
            cout << "SEMANTIC ERROR: Relation already exists" << endl;
            return false;
        }

        if (!isFileExists(parsedQuery.loadRelationName))
        {
            cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
            return false;
        }
    }
    else
    {
        if (matrixCatalogue.isMatrix(parsedQuery.loadMatrixName))
        {
            cout << "SEMANTIC ERROR: Matrix already exists" << endl;
            return false;
        }

        if (!isFileExists(parsedQuery.loadMatrixName))
        {
            cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
            return false;
        }
    }

    return true;
}

void executeLOAD()
{
    logger.log("executeLOAD");
    if (tokenizedQuery.size() == 2)
    {

        Table *table = new Table(parsedQuery.loadRelationName);
        if (table->load())
        {
            tableCatalogue.insertTable(table);
            cout << "Loaded Table. Column Count: " << table->columnCount << " Row Count: " << table->rowCount << endl;
        }
    }
    else
    {
        Matrix *matrix = new Matrix(parsedQuery.loadMatrixName);
        if (matrix->load())
        {
            matrixCatalogue.insertMatrix(matrix);
            cout << "Loaded matrix. Column Count: " << matrix->columnCount << " Row Count: " << matrix->rowCount << endl;
        }
    }

    return;
}