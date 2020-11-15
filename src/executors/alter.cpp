#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- SELECT column_name bin_op [column_name | int_literal] FROM relation_name
 */
bool syntacticParseALTER()
{
    logger.log("syntacticParseALTER");
    if (tokenizedQuery.size() != 6 || tokenizedQuery[0] != "ALTER" || tokenizedQuery[1] != "TABLE" || (tokenizedQuery[3] != "ADD" && tokenizedQuery[3] != "DELETE") ||
        tokenizedQuery[4] != "COLUMN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = ALTER;
    // parsedQuery.selectionResultRelationName = tokenizedQuery[2];
    // parsedQuery.selectionFirstColumnName = tokenizedQuery[3];
    parsedQuery.alterRelationName = tokenizedQuery[2];
    parsedQuery.alterColumnName = tokenizedQuery[5];
    parsedQuery.alterMethod = tokenizedQuery[3];
    return true;
}

bool semanticParseALTER()
{
    logger.log("semanticParseALTER");

    if (!tableCatalogue.isTable(parsedQuery.alterRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    Table *table = tableCatalogue.getTable(parsedQuery.alterRelationName);
    if (parsedQuery.alterMethod == "DELETE" && find(table->columns.begin(), table->columns.end(), parsedQuery.alterColumnName) == table->columns.end())
    {
        cout << "SEMANTIC ERROR: @TA dekh k dalo bhai." << endl;
        return false;
    }
    if (parsedQuery.alterMethod == "ADD" && find(table->columns.begin(), table->columns.end(), parsedQuery.alterColumnName) != table->columns.end())
    {
        cout << "SEMANTIC ERROR: @TA kitni baar daloge?" << endl;
        return false;
    }
    return true;
}

void executeALTER()
{
    Table *table = tableCatalogue.getTable(parsedQuery.alterRelationName);
    Cursor cursor = table->getCursor();
    if (parsedQuery.alterMethod == "ADD")
    {

        int blockCount = 0;
        while (true)
        {
            vector<vector<int>> blockContents = cursor.getBlock(1);
            if (blockContents.size() == 0)
                break;
            for (int i = 0; i < blockContents.size(); i++)
            {
                blockContents[i].push_back(0);
            }
            bufferManager.writePage(table->tableName, blockCount++, blockContents, blockContents.size());
        }
        table->columns.push_back(parsedQuery.alterColumnName);
        table->columnCount += 1;
    }
    else
    {
        int columnIndex = table->getColumnIndex(parsedQuery.alterColumnName);
        int blockCount = 0;
        while (true)
        {
            vector<vector<int>> blockContents = cursor.getBlock(1);
            if (blockContents.size() == 0)
                break;
            for (int i = 0; i < blockContents.size(); i++)
            {
                blockContents[i].erase(blockContents[i].begin() + columnIndex);
            }
            bufferManager.writePage(table->tableName, blockCount++, blockContents, blockContents.size());
        }
        table->columns.erase(table->columns.begin() + columnIndex);

        table->columnCount -= 1;
        if (table->columns.size() == 0)
            tableCatalogue.deleteTable(table->tableName);
    }
}
