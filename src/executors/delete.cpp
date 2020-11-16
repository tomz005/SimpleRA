#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- SELECT column_name bin_op [column_name | int_literal] FROM relation_name
 */
bool syntacticParseDELETE()
{
    logger.log("syntacticParseDELETE");

    if (tokenizedQuery.size() < 5 || tokenizedQuery[0] != "DELETE" || tokenizedQuery[1] != "FROM" || tokenizedQuery[3] != "VALUES")
    {
        cout << "DELETE : SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = DELETE;
    // parsedQuery.selectionResultRelationName = tokenizedQuery[2];
    // parsedQuery.selectionFirstColumnName = tokenizedQuery[3];
    parsedQuery.deleteRelationName = tokenizedQuery[2];
    parsedQuery.deleteVector.clear();
    for (int i = 4; i < tokenizedQuery.size(); i++)
    {
        try
        {
            parsedQuery.deleteVector.push_back(stoi(tokenizedQuery[i]));
        }
        catch (...)
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }

    return true;
}

bool semanticParseDELETE()
{
    logger.log("semanticParseDELETE");

    if (!tableCatalogue.isTable(parsedQuery.deleteRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    Table *table = tableCatalogue.getTable(parsedQuery.deleteRelationName);
    if (table->columnCount != parsedQuery.deleteVector.size())
    {
        cout << "SEMANTIC ERROR : Columns dont match" << endl;
        return false;
    }
    return true;
}

void executeDELETE()
{
    logger.log("executeDELETE");
    Table *table = tableCatalogue.getTable(parsedQuery.deleteRelationName);
    vector<vector<int>> storedRows;
    Cursor cursor = table->getCursor();
    int blockcount = 0;
    while (true)
    {
        vector<vector<int>> blocks = cursor.getBlock(1);
        if (blocks.size() == 0)
            break;
        for (int i = 0; i < blocks.size(); i++)
        {
            if (blocks[i] == parsedQuery.deleteVector)
                continue;
            storedRows.push_back(blocks[i]);
            if (storedRows.size() == table->maxRowsPerBlock)
            {
                bufferManager.writePage(table->tableName, blockcount++, storedRows, storedRows.size());
                storedRows.clear();
            }
        }
    }
    if (storedRows.size() != 0)
    {
        bufferManager.writePage(table->tableName, blockcount++, storedRows, storedRows.size());
        table->rowsPerBlockCount[blockcount - 1] = storedRows.size();
        storedRows.clear();
    }
    if (blockcount == 0)
    {
        tableCatalogue.deleteTable(table->tableName);
        return;
    }
    for (int i = blockcount; i < table->blockCount; i++)
    {
        bufferManager.deleteFile(table->tableName, i);
        table->rowsPerBlockCount.pop_back();
    }
    table->blockCount = blockcount;
    int rowcount = 0;
    rowcount = accumulate(table->rowsPerBlockCount.begin(), table->rowsPerBlockCount.end(), 0);
    table->rowCount = rowcount;
}