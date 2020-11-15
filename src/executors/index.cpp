#include "global.h"
/**
 * @brief 
 * SYNTAX: INDEX ON column_name FROM relation_name USING indexing_strategy
 * indexing_strategy: ASC | DESC | NOTHING
 */
bool syntacticParseINDEX()
{
    logger.log("syntacticParseINDEX");
    bool isValid = false;
    if (tokenizedQuery.size() == 9 && (tokenizedQuery[7] == "FANOUT" || tokenizedQuery[7] == "BUCKETS"))
        isValid = true;
    if (tokenizedQuery.size() != 9 || tokenizedQuery[1] != "ON" || tokenizedQuery[3] != "FROM" || tokenizedQuery[5] != "USING" || !isValid)
    {
        // cout << isValid << endl;
        // cout << tokenizedQuery.size() << endl;
        // for (int i = 0; i < tokenizedQuery.size(); i++)
        //     cout << tokenizedQuery[i] << endl;
        // cout << ~(tokenizedQuery[7] != "BUCKETS" ^ tokenizedQuery[7] != "FANOUT") << endl;
        if (tokenizedQuery.size() != 7 || tokenizedQuery[1] != "ON" || tokenizedQuery[3] != "FROM" || tokenizedQuery[5] != "USING" || tokenizedQuery[6] != "NOTHING")
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }
    parsedQuery.queryType = INDEX;
    parsedQuery.indexColumnName = tokenizedQuery[2];
    parsedQuery.indexRelationName = tokenizedQuery[4];
    string indexingStrategy = tokenizedQuery[6];
    if (indexingStrategy == "BTREE")
    {
        parsedQuery.indexingStrategy = BTREE;
        parsedQuery.indexStrategyCount = stoi(tokenizedQuery[8]);
    }
    else if (indexingStrategy == "HASH")
    {
        parsedQuery.indexingStrategy = HASH;
        parsedQuery.indexStrategyCount = stoi(tokenizedQuery[8]);
    }
    else if (indexingStrategy == "NOTHING")
        parsedQuery.indexingStrategy = NOTHING;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseINDEX()
{
    logger.log("semanticParseINDEX");
    if (!tableCatalogue.isTable(parsedQuery.indexRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    if (!tableCatalogue.isColumnFromTable(parsedQuery.indexColumnName, parsedQuery.indexRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    Table *table = tableCatalogue.getTable(parsedQuery.indexRelationName);
    if (table->indexed)
    {
        if (parsedQuery.indexingStrategy != NOTHING)
        {
            cout << "SEMANTIC ERROR: Table already indexed" << endl;
            return false;
        }
        if (table->indexedColumn != parsedQuery.indexColumnName)
        {
            cout << "SEMANTIC ERROR: Column specified is not indexed" << endl;
            return false;
        }
    }
    if (parsedQuery.indexingStrategy != NOTHING && parsedQuery.indexStrategyCount < 1)
    {
        cout << "Bucket/Child size less than 1" << endl;
        return false;
    }
    return true;
}

bool getRow1(fstream &fin, vector<int> &row)
{
    string line, val;
    if (!getline(fin, line))
    {
        return false;
    }
    stringstream ss(line);
    int rowidx = 0;
    while (getline(ss, val, ' '))
    {
        row[rowidx++] = stoi(val);
    }
    return true;
}

void sort_for_index(map<int, vector<int>> &indexTable)
{
    Table *resultantTable = tableCatalogue.getTable(parsedQuery.indexRelationName);
    system("mkdir ../data/temp/Phase1");
    int blockCount = 0;
    resultantTable->indexed = true;
    Cursor cursor = resultantTable->getCursor();
    resultantTable->indexedColumn = parsedQuery.indexColumnName;
    int ColumnIndex = resultantTable->getColumnIndex(parsedQuery.indexColumnName);
    while (true)
    {
        vector<vector<int>> sortedBlock = cursor.getBlock(1);
        if (sortedBlock.empty())
            break;
        // asc = parsedQuery.sortingStrategy == ASC;
        sort(sortedBlock.begin(), sortedBlock.end(), [&](vector<int> &a, vector<int> &b) {
            return a[ColumnIndex] <= b[ColumnIndex];
        });
        bufferManager.writePage("/Phase1/", blockCount++, sortedBlock, sortedBlock.size());
    }
    fstream fin[blockCount];
    vector<int> row(resultantTable->columnCount);
    for (int i = 0; i < blockCount; i++)
        fin[i].open("../data/temp/Phase1/_Page" + to_string(i), ios::in);
    auto lambda = [&](pair<int, vector<int>> &a, pair<int, vector<int>> &b) {
        return a.second[ColumnIndex] >= b.second[ColumnIndex];
    };
    priority_queue<pair<int, vector<int>>, vector<pair<int, vector<int>>>, decltype(lambda)> pq(lambda);
    // ofstream out(resultantTable->sourceFileName);
    for (int i = 0; i < blockCount; i++)
    {
        getRow1(fin[i], row);
        pq.push({i, row});
    }
    vector<vector<int>> sortedPage;
    int idx = 0;
    int rowNumber = 0;
    while (!pq.empty())
    {
        auto t = pq.top();
        pq.pop();
        if (indexTable.find(t.second[ColumnIndex]) == indexTable.end())
        {
            indexTable[t.second[ColumnIndex]].push_back(idx);
            indexTable[t.second[ColumnIndex]].push_back(rowNumber);
            indexTable[t.second[ColumnIndex]].push_back(idx);
            indexTable[t.second[ColumnIndex]].push_back(rowNumber);
        }
        indexTable[t.second[ColumnIndex]][2] = idx;
        indexTable[t.second[ColumnIndex]][3] = rowNumber;
        sortedPage.push_back(t.second);
        // resultantTable->writeRow<int>(t.second);
        // cout << resultantTable->maxRowsPerBlock << "---------------" << endl;
        rowNumber++;
        if (sortedPage.size() == resultantTable->maxRowsPerBlock)
        {
            rowNumber = 0;
            bufferManager.writePage(resultantTable->tableName, idx++, sortedPage, sortedPage.size());
            sortedPage.clear();
        }
        if (getRow1(fin[t.first], row) == false)
            continue;

        pq.push({t.first, row});
    }
    if (sortedPage.size() > 0)
    {
        bufferManager.writePage(resultantTable->tableName, idx++, sortedPage, sortedPage.size());
    }
    // tableCatalogue.insertTable(resultantTable);
    resultantTable->indexTable = indexTable;
    system("rm -rf ../data/temp/Phase1");
}
void executeINDEX()
{
    logger.log("executeINDEX");
    map<int, vector<int>> indexTable;
    if (parsedQuery.indexingStrategy != NOTHING)
        sort_for_index(indexTable);
    else
    {
        Table *resultantTable = tableCatalogue.getTable(parsedQuery.indexRelationName);
        resultantTable->indexed = false;
        resultantTable->indexTable.clear();
    }

    return;
}