#include "global.h"
/**
 * @brief 
 * SYNTAX: TRANSPOSE relation_name
 */
bool syntacticParseTRANSPOSE()
{
    logger.log("syntacticParseTRANSPOSE");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    else
    {
        parsedQuery.queryType = TRANSPOSE;
        parsedQuery.loadMatrixName = tokenizedQuery[1];
        return true;
    }
}

bool semanticParseTRANSPOSE()
{
    logger.log("semanticParseTRANSPOSE");
    if (!matrixCatalogue.isMatrix(parsedQuery.loadMatrixName))
    {
        cout << "SEMANTIC ERROR: Matrix doesnt exists" << endl;
        return false;
    }

    return true;
}

void executeTRANSPOSE()
{
    logger.log("executeTRANSPOSE");
    Matrix *matrix = matrixCatalogue.getMatrix(parsedQuery.loadMatrixName);
    int blockNumber1, blockNumber2;
    // Page p1, p2;
    for (int rowCounter = 1; rowCounter <= matrix->rowCount; rowCounter++)
    {
        for (int columnCounter = rowCounter + 1; columnCounter <= matrix->columnCount; columnCounter++)
        {

            if (columnCounter % matrix->maxColumnsPerBlock == 0)
                blockNumber1 = columnCounter / matrix->maxColumnsPerBlock;
            else
                blockNumber1 = (columnCounter / matrix->maxColumnsPerBlock) + 1;

            if (rowCounter % matrix->maxColumnsPerBlock == 0)
                blockNumber2 = rowCounter / matrix->maxColumnsPerBlock;
            else
                blockNumber2 = (rowCounter / matrix->maxColumnsPerBlock) + 1;

            // p1 = bufferManager.getPage(matrix->matrixName, rowCounter, blockNumber1);
            // p2 = bufferManager.getPage(matrix->matrixName, columnCounter, blockNumber2);
            vector<int> r1 = Page(matrix->matrixName, to_string(rowCounter) + "_" + to_string(blockNumber1)).getRow(0);

            vector<int> r2 = Page(matrix->matrixName, to_string(columnCounter) + "_" + to_string(blockNumber2)).getRow(0);
            int idx1, idx2;
            idx1 = (columnCounter - 1) % matrix->maxColumnsPerBlock;
            idx2 = (rowCounter - 1) % matrix->maxColumnsPerBlock;
            // vector<int> r1 = p1.getRow(0);
            // vector<int> r2 = p2.getRow(0);
            // cout << "------------------" << endl;
            // cout << rowCounter << endl;
            // cout << columnCounter << endl;
            // cout << blockNumber1 << endl;
            // cout << blockNumber2 << endl;
            // for (int i = 0; i < r1.size(); i++)
            // {
            //     cout << r1[i] << " ";
            // }
            // cout << endl;
            // for (int i = 0; i < r2.size(); i++)
            //     cout << r2[i] << " ";
            // cout << endl;
            swap(r1[idx1], r2[idx2]);
            Page p1(matrix->matrixName, to_string(rowCounter) + "_" + to_string(blockNumber1), vector<vector<int>>(1, r1), r1.size());
            Page p2(matrix->matrixName, to_string(columnCounter) + "_" + to_string(blockNumber2), vector<vector<int>>(1, r2), r2.size());
            p1.writePage();
            p2.writePage();
        }
    }

    return;
}