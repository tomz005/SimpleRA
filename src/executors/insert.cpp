#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- SELECT column_name bin_op [column_name | int_literal] FROM relation_name
 */
bool syntacticParseINSERT()
{
    logger.log("syntacticParseINSERT");
    if (tokenizedQuery.size() < 5 || tokenizedQuery[0] != "INSERT" || tokenizedQuery[1] != "INTO" || tokenizedQuery[3] != "VALUES")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = INSERT;
    // parsedQuery.selectionResultRelationName = tokenizedQuery[2];
    // parsedQuery.selectionFirstColumnName = tokenizedQuery[3];
    parsedQuery.insertRelationName = tokenizedQuery[2];
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
    Table *table = tableCatalogue.getTable(parsedQuery.insertRelationName);
    string oldfilename = table->sourceFileName;
    string newfilename = "../data/temp/" + table->tableName + ".csv";
    string command = "cp " + oldfilename + " " + newfilename;
    system(command.c_str());
    table->sourceFileName = newfilename;
}
