#include "global.h"
/**
 * @brief 
 * SYNTAX: INSERT​ ​ INTO​ <table_name> ​ VALUES​ <value1>[,<value2>]*
 */
bool syntacticParseINSERT()
{
    logger.log("syntacticParseINSERT");
    // for (auto q : tokenizedQuery)
    //     cout << q << endl;
    // cout << (int)(tokenizedQuery[0] != "INSERT") << endl;
    // cout << (int)(tokenizedQuery[1] != "INTO") << endl;
    // cout << (int)(tokenizedQuery[3] != "VALUES") << endl;
    // cout << (int)(tokenizedQuery.size() < 5) << endl;

    if (tokenizedQuery.size() < 5 || tokenizedQuery[0] != "INSERT" || tokenizedQuery[1] != "INTO" || tokenizedQuery[3] != "VALUES")
    {
        cout << "INSERT : SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = INSERT;
    // parsedQuery.selectionResultRelationName = tokenizedQuery[2];
    // parsedQuery.selectionFirstColumnName = tokenizedQuery[3];
    parsedQuery.insertRelationName = tokenizedQuery[2];
    parsedQuery.insertVector.clear();
    for (int i = 4; i < tokenizedQuery.size(); i++)
    {
        try
        {
            parsedQuery.insertVector.push_back(stoi(tokenizedQuery[i]));
        }
        catch (...)
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }

    return true;
}

bool semanticParseINSERT()
{
    logger.log("semanticParseINSERT");

    if (!tableCatalogue.isTable(parsedQuery.insertRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    Table *table = tableCatalogue.getTable(parsedQuery.insertRelationName);
    if (table->columnCount != parsedQuery.insertVector.size())
    {
        cout << "SEMANTIC ERROR : Columns dont match" << endl;
        return false;
    }
    return true;
}

void executeINSERT()
{
    logger.log("executeINSERT");
    Table *table = tableCatalogue.getTable(parsedQuery.insertRelationName);
    // Table *resultantTable = new Table(table->tableName, table->columns);
    Cursor cursor = Cursor(table->tableName, table->blockCount - 1);
    vector<vector<int>> blocks = cursor.getBlock(1);
    if (blocks.size() == table->maxRowsPerBlock)
    {
        bufferManager.writePage(table->tableName, table->blockCount, vector<vector<int>>(1, parsedQuery.insertVector), 1);
        table->blockCount += 1;
        table->rowsPerBlockCount.push_back(1);
    }
    else
    {
        blocks.push_back(parsedQuery.insertVector);
        bufferManager.writePage(table->tableName, (table->blockCount) - 1, blocks, blocks.size());
        table->rowsPerBlockCount[table->blockCount - 1] += 1;
    }

    table->rowCount += 1;
}
