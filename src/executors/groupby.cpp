#include "global.h"
/**
 * @brief 
 * SYNTAX: <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> RETURN MAX|MIN|SUM|AVG(<attribute>)
 */
bool syntacticParseGROUPBY()
{
    logger.log("syntacticParseGROUPBY");
    if (tokenizedQuery.size() != 9 || tokenizedQuery[2] != "GROUP" || tokenizedQuery[3] != "BY" || tokenizedQuery[5] != "FROM" || tokenizedQuery[7] != "RETURN" ||
        tokenizedQuery[8][3] != '(' || tokenizedQuery[8].back() != ')')
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    string strategy = tokenizedQuery[8].substr(0, 3);
    if (strategy != "MIN" && strategy != "MAX" && strategy != "AVG" && strategy != "SUM")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    string columnname = tokenizedQuery[8].substr(4);
    columnname.pop_back();
    if (columnname.empty())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = GROUPBY;
    if (strategy == "MAX")
        parsedQuery.groupingstrategy = MAX;
    else if (strategy == "MIN")
        parsedQuery.groupingstrategy = MIN;
    else if (strategy == "AVG")
        parsedQuery.groupingstrategy = AVG;
    else
        parsedQuery.groupingstrategy = SUM;
    parsedQuery.groupbyGroupColumnName = columnname;
    parsedQuery.groupbyResultantColumnName = tokenizedQuery[4];
    parsedQuery.groupbyRelationName = tokenizedQuery[6];
    parsedQuery.groupbyResultantRelationName = tokenizedQuery[0];
    return true;
}

bool semanticParseGROUPBY()
{
    logger.log("semanticParseGROUPBY");

    if (!tableCatalogue.isTable(parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    if (tableCatalogue.isTable(parsedQuery.groupbyResultantRelationName))
    {
        cout << "SEMANTIC ERROR: Target Relation already exist" << endl;
        return false;
    }
    // Table *table = tableCatalogue.getTable(parsedQuery.groupbyRelationName);
    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupbyGroupColumnName, parsedQuery.groupbyRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.groupbyResultantColumnName, parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR : COLUMNS doesn't exist" << endl;
        return false;
    }
    return true;
}

void executeGROUPBY()
{
    logger.log("executeGROUPBY");
    vector<string> columnnames;
    columnnames.push_back(parsedQuery.groupbyResultantColumnName);
    switch (parsedQuery.groupingstrategy)
    {
    case MAX:
        columnnames.push_back("MAX" + parsedQuery.groupbyGroupColumnName);
        break;
    case MIN:
        columnnames.push_back("MIN" + parsedQuery.groupbyGroupColumnName);
        break;
    case AVG:
        columnnames.push_back("AVG" + parsedQuery.groupbyGroupColumnName);
        break;
    default:
        columnnames.push_back("SUM" + parsedQuery.groupbyGroupColumnName);
        break;
    }
    // cout << columnnames.size() << endl;
    Table *resultantTable = new Table(parsedQuery.groupbyResultantRelationName, columnnames);
    Table *table = tableCatalogue.getTable(parsedQuery.groupbyRelationName);
    unordered_map<int, pair<int, int>> mp;
    int groupColumnindex = table->getColumnIndex(parsedQuery.groupbyResultantColumnName);
    int destinationColumnindex = table->getColumnIndex(parsedQuery.groupbyGroupColumnName);
    Cursor cursor = table->getCursor();
    while (true)
    {
        vector<vector<int>> block = cursor.getBlock(1);
        if (block.size() == 0)
            break;
        for (auto rows : block)
        {
            if (mp.find(rows[groupColumnindex]) == mp.end())
                mp[rows[groupColumnindex]] = {rows[destinationColumnindex], 1};
            else
            {
                switch (parsedQuery.groupingstrategy)
                {
                case MIN:
                    mp[rows[groupColumnindex]].first = min(mp[rows[groupColumnindex]].first,
                                                           rows[destinationColumnindex]);
                    break;
                case MAX:
                    mp[rows[groupColumnindex]].first = max(mp[rows[groupColumnindex]].first,
                                                           rows[destinationColumnindex]);
                    break;
                default:
                    mp[rows[groupColumnindex]].first += rows[destinationColumnindex];
                    mp[rows[groupColumnindex]].second += 1;
                }
            }
        }
    }

    for (auto entry : mp)
    {
        if (parsedQuery.groupingstrategy == AVG)
            resultantTable->writeRow<int>({entry.first, entry.second.first / entry.second.second});
        else
            resultantTable->writeRow<int>({entry.first, entry.second.first});
    }
    if (resultantTable->blockify())
        tableCatalogue.insertTable(resultantTable);
}
