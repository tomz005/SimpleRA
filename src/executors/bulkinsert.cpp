#include "global.h"
/**
 * @brief 
 * SYNTAX: BULK_INSERT <csv_file_name> INTO <table_name>
 */
bool syntacticParseBULKINSERT()
{
    logger.log("syntacticParseBULKINSERT");
    if (tokenizedQuery.size() != 4 || tokenizedQuery[0] != "BULK_INSERT" || tokenizedQuery[2] != "INTO")
    {
        cout << "Bluk : SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = BULKINSERT;
    parsedQuery.bulkinsertRelationName = tokenizedQuery[1];
    parsedQuery.bulkinsertResultantRelationName = tokenizedQuery[3];
    parsedQuery.bulkinsertsourceTable = new Table(parsedQuery.bulkinsertRelationName);
    // parsedQuery.bulkinsertsourceTable.load();
    return true;
}

bool semanticParseBULKINSERT()
{
    logger.log("semanticParseBULKINSERT");

    if (!tableCatalogue.isTable(parsedQuery.bulkinsertResultantRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    if (tableCatalogue.isTable(parsedQuery.bulkinsertRelationName))
    {
        cout << "SEMANTIC ERROR: Source Relation already exists as Table" << endl;
        return false;
    }
    parsedQuery.bulkinsertsourceTable->load();
    Table *table = tableCatalogue.getTable(parsedQuery.bulkinsertResultantRelationName);
    if (table->columnCount != parsedQuery.bulkinsertsourceTable->columnCount)
        return false;
    for (int i = 0; i < table->columnCount; i++)
    {
        if (table->columns[i] != parsedQuery.bulkinsertsourceTable->columns[i])
        {
            cout << "SEMANTIC ERROR: Column Heading mismatch" << endl;
            return false;
        }
    }
    return true;
}

void executeBULKINSERT()
{
    logger.log("executeBULKINSERT");
    Table *table = tableCatalogue.getTable(parsedQuery.bulkinsertResultantRelationName);
    tableCatalogue.insertTable(parsedQuery.bulkinsertsourceTable);
    int lastblockrows = (table->rowCount) % (table->maxRowsPerBlock);
    Cursor cursor = parsedQuery.bulkinsertsourceTable->getCursor();
    Cursor c = Cursor(table->tableName, table->blockCount - 1);
    // c.pageIndex = (table->blockCount) - 1;
    // cout << table->blockCount << endl;
    vector<vector<int>> values;
    if (lastblockrows != 0)
    {
        vector<vector<int>> lastblockrows = c.getBlock(1);
        vector<vector<int>> firstblock = cursor.getBlock(1);
        int i = 0;
        for (; i < firstblock.size() && lastblockrows.size() != table->maxRowsPerBlock; i++)
        {
            lastblockrows.push_back(firstblock[i]);
        }
        for (; i < firstblock.size(); i++)
            values.push_back(firstblock[i]);
        bufferManager.writePage(table->tableName, (table->blockCount) - 1, lastblockrows, lastblockrows.size());
        table->rowsPerBlockCount[table->blockCount - 1] = lastblockrows.size();
    }
    int blockCount = table->blockCount;
    while (true)
    {
        vector<vector<int>> fresh = cursor.getBlock(1);
        if (fresh.size() == 0)
            break;
        int i = 0;
        for (; i < fresh.size() && values.size() != table->maxRowsPerBlock; i++)
            values.push_back(fresh[i]);
        bufferManager.writePage(table->tableName, blockCount++, values, values.size());
        table->rowsPerBlockCount.push_back(values.size());
        values.clear();
        for (; i < fresh.size(); i++)
            values.push_back(fresh[i]);
    }
    if (values.size() != 0)
    {
        bufferManager.writePage(table->tableName, blockCount++, values, values.size());
        table->rowsPerBlockCount.push_back(values.size());
    }
    for (auto lm : table->rowsPerBlockCount)
        cout << lm << ",";
    cout << endl;
    table->blockCount = blockCount;
    table->rowCount += parsedQuery.bulkinsertsourceTable->rowCount;
    tableCatalogue.deleteTable(parsedQuery.bulkinsertRelationName);
}
