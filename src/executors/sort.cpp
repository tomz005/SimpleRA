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
    if (table.indexed && table.indexedColumn == parsedQuery.sortColumnName)
    {
        // cout << "Already indexed" << endl;
        int blockCount = (parsedQuery.sortingStrategy == ASC) ? 0 : table.blockCount;
        vector<vector<int>> previousRows;
        while (true)
        {
            vector<vector<int>> sortedBlock = cursor.getBlock(1);
            if (sortedBlock.empty())
                break;
            // for (auto entry : sortedBlock)
            //     resultantTable->writeRow<int>(entry);
            if (parsedQuery.sortingStrategy == DESC)
            {
                reverse(sortedBlock.begin(), sortedBlock.end());
                int rowsBlock = resultantTable->maxRowsPerBlock;
                if (blockCount == table.blockCount)
                {
                    rowsBlock = (resultantTable->rowCount) % (resultantTable->maxRowsPerBlock);
                    if (rowsBlock == 0 && resultantTable->rowCount != 0)
                        rowsBlock = resultantTable->maxRowsPerBlock;
                }
                for (auto rows : previousRows)
                {
                    sortedBlock.push_back(rows);
                }
                previousRows.clear();
                for (int i = sortedBlock.size() - rowsBlock; i < sortedBlock.size(); i++)
                {
                    previousRows.push_back(sortedBlock[i]);
                }
                bufferManager.writePage(resultantTable->tableName, --blockCount, previousRows, previousRows.size());
                previousRows.clear();
                for (int i = 0; i < sortedBlock.size() - rowsBlock; i++)
                {
                    previousRows.push_back(sortedBlock[i]);
                }
            }
            else
                bufferManager.writePage(resultantTable->tableName, blockCount++, sortedBlock, sortedBlock.size());
        }
        // cout << "Write page executed\n";
        tableCatalogue.insertTable(resultantTable);

        Cursor c = resultantTable->getCursor();
        // logger.log("Cursor of resultant table");
        while (true)
        {
            vector<vector<int>> sortedBlock = c.getBlock(1);
            if (sortedBlock.empty())
                break;
            for (auto entry : sortedBlock)
            {
                for (int g : entry)
                    cout << g << ",";
                cout << endl;
                resultantTable->writeRow<int>(entry);
            }
        }
    }
    else
    {
        if (tokenizedQuery.size() == 8)
        {
            parsedQuery.sortBufferSize = 1;
        }
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