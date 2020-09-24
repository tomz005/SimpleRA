#include "global.h"
/**
 * @brief File contains method to process SORT commands.
 * 
 * syntax:
 * R <- SORT relation_name BY column_name IN sorting_order
 * 
 * sorting_order = ASC | DESC 
 */
bool syntacticParseSORT()
{
    logger.log("syntacticParseSORT");
    if ((tokenizedQuery.size() != 8 && tokenizedQuery.size() != 10) || tokenizedQuery[4] != "BY" || tokenizedQuery[6] != "IN" ||
        (tokenizedQuery.size() == 10 && tokenizedQuery[8] != "BUFFER"))
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = SORT;
    parsedQuery.sortResultRelationName = tokenizedQuery[0];
    parsedQuery.sortColumnName = tokenizedQuery[5];
    parsedQuery.sortRelationName = tokenizedQuery[3];
    string sortingStrateg = tokenizedQuery[7];
    if (sortingStrateg == "ASC")
        parsedQuery.sortingStrategy = ASC;
    else if (sortingStrateg == "DESC")
        parsedQuery.sortingStrategy = DESC;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    if (tokenizedQuery.size() == 10)
    {
        try
        {

            parsedQuery.sortBufferSize = stoi(tokenizedQuery[9]);
        }
        catch (...)
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }
    return true;
}

bool semanticParseSORT()
{
    logger.log("semanticParseSORT");

    if (tableCatalogue.isTable(parsedQuery.sortResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.sortColumnName, parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    if (parsedQuery.sortBufferSize <= 0 && tokenizedQuery.size() == 10)
    {
        cout << "SEMANTIC ERROR : Buffer size <=0 " << endl;
        return false;
    }
    return true;
}

bool getRow(fstream &fin, vector<int> &row)
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

void executeSORT()
{
    logger.log("executeSORT");
    Table table = *tableCatalogue.getTable(parsedQuery.sortRelationName);
    Table *resultantTable = new Table(parsedQuery.sortResultRelationName, table.columns);
    resultantTable->distinctValuesPerColumnCount = table.distinctValuesPerColumnCount;
    resultantTable->rowCount = table.rowCount;
    resultantTable->blockCount = table.blockCount;
    resultantTable->rowsPerBlockCount = table.rowsPerBlockCount;
    Cursor cursor = table.getCursor();
    bool asc;
    // vector<int> row = cursor.getNext();
    int ColumnIndex = table.getColumnIndex(parsedQuery.sortColumnName);
    if (tokenizedQuery.size() == 8)
    {
    }
    else
    {
        system("mkdir ../data/temp/Phase1");
        int blockCount = 0;
        while (true)
        {
            vector<vector<int>> sortedBlock = cursor.getBlock(parsedQuery.sortBufferSize);
            if (sortedBlock.empty())
                break;
            asc = parsedQuery.sortingStrategy == ASC;
            sort(sortedBlock.begin(), sortedBlock.end(), [&](vector<int> &a, vector<int> &b) {
                if (asc)
                {
                    return a[ColumnIndex] <= b[ColumnIndex];
                }
                else
                {
                    return a[ColumnIndex] >= b[ColumnIndex];
                }
            });
            // for (int i = 0; i < ans.size(); i++)
            // {
            //     for (int j = 0; j < ans[0].size(); j++)
            //     {
            //         cout << ans[i][j] << ",";
            //     }
            //     cout << endl;
            // }
            bufferManager.writePage("/Phase1/", blockCount++, sortedBlock, sortedBlock.size());
        }
        fstream fin[blockCount];
        vector<int> row(resultantTable->columnCount);
        for (int i = 0; i < blockCount; i++)
            fin[i].open("../data/temp/Phase1/_Page" + to_string(i), ios::in);
        auto lambda = [&](pair<int, vector<int>> &a, pair<int, vector<int>> &b) {
            if (asc)
            {
                return a.second[ColumnIndex] >= b.second[ColumnIndex];
            }
            else
            {
                return a.second[ColumnIndex] <= b.second[ColumnIndex];
            }
        };
        priority_queue<pair<int, vector<int>>, vector<pair<int, vector<int>>>, decltype(lambda)> pq(lambda);
        // ofstream out(resultantTable->sourceFileName);
        for (int i = 0; i < blockCount; i++)
        {
            getRow(fin[i], row);
            pq.push({i, row});
        }
        vector<vector<int>> sortedPage;
        int idx = 0;
        while (!pq.empty())
        {
            auto t = pq.top();
            pq.pop();
            // cout << t.first << endl;
            // for (int i = 0; i < t.second.size(); i++)
            //     cout << t.second[i] << ",";
            // cout << endl;
            sortedPage.push_back(t.second);
            resultantTable->writeRow<int>(t.second);
            // cout << resultantTable->maxRowsPerBlock << "---------------" << endl;
            if (sortedPage.size() == resultantTable->maxRowsPerBlock)
            {
                bufferManager.writePage(resultantTable->tableName, idx++, sortedPage, sortedPage.size());
                sortedPage.clear();
            }
            if (getRow(fin[t.first], row) == false)
                continue;

            pq.push({t.first, row});
        }
        if (sortedPage.size() > 0)
        {
            bufferManager.writePage(resultantTable->tableName, idx++, sortedPage, sortedPage.size());
        }
        tableCatalogue.insertTable(resultantTable);
        system("rm -rf ../data/temp/Phase1");
    }
    return;
}