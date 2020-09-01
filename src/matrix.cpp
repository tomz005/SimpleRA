#include "global.h"

/**
 * @brief Construct a new Matrix:: Matrix object
 *
 */
Matrix::Matrix()
{
    logger.log("Matrix::Matrix");
}

/**
 * @brief Construct a new Matrix:: Matrix object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param tableName 
 */
Matrix::Matrix(string matrixName)
{
    logger.log("Matrix::Matrix");
    this->sourceFileName = "../data/" + matrixName + ".csv";
    this->matrixName = matrixName;
}

/**
 * @brief Construct a new Matrix:: Matrix object used when an assignment command
 * is encountered. To create the table object both the table name and the
 * columns the table holds should be specified.
 *
 * @param tableName 
 * @param columns 
 */
// Matrix::Matrix(string tableName)
// {
//     logger.log("Matrix::Matrix");
//     this->sourceFileName = "../data/temp/" + tableName + ".csv";
//     this->tableName = tableName;
//     this->columns = columns;
//     this->columnCount = columns.size();
//     this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (32 * columnCount));
//     this->writeRow<string>(columns);
// }

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates table
 * statistics.
 *
 * @return true if the table has been successfully loaded 
 * @return false if an error occurred 
 */
bool Matrix::load()
{
    logger.log("Matrix::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();
        if (this->extractColumnNames(line))
            if (this->blockify())
                return true;
        return false;
    }
    fin.close();
    return false;
}

/**
 * @brief Function extracts column names from the header line of the .csv data
 * file. 
 *
 * @param line 
 * @return true if column names successfully extracted (i.e. no column name
 * repeats)
 * @return false otherwise
 */
bool Matrix::extractColumnNames(string firstLine)
{
    logger.log("Matrix::extractColumnNames");
    // unordered_set<string> columnNames;
    string word;
    stringstream s(firstLine);
    this->columnCount = 0;
    this->rowCount = 0;
    while (getline(s, word, ','))
    {
        this->columnCount += 1;
        // word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        // if (columnNames.count(word))
        //     return false;
        // columnNames.insert(word);
        // this->columns.emplace_back(word);
    }
    // this->columnCount = this->columns.size();
    // this->maxColumnsPerBlock =

    fstream fin(this->sourceFileName, ios::in);
    string line;
    while (getline(fin, line))
        this->rowCount += 1;
    fin.close();
    if (this->rowCount != this->columnCount)
        return false;
    if ((uint)((BLOCK_SIZE * 1000) / (32 * this->columnCount)) == 0)
        this->maxColumnsPerBlock = BLOCK_SIZE * 1000;
    else
        this->maxColumnsPerBlock = this->columnCount;

    this->blocksPerRow = ceil((double)this->columnCount / this->maxColumnsPerBlock);
    logger.log(to_string(this->maxColumnsPerBlock));
    logger.log(to_string(this->blocksPerRow));
    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size. 
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Matrix::blockify()
{
    logger.log("Matrix::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    vector<int> row(this->maxColumnsPerBlock, 0);
    vector<vector<int>> rowsInPage(1, row);
    int rowCounter = 0;
    // unordered_set<int> dummy;
    // dummy.clear();
    // this->distinctValuesInColumns.assign(this->columnCount, dummy);
    // this->distinctValuesPerColumnCount.assign(this->columnCount, 0);
    // getline(fin, line);
    while (getline(fin, line))
    {
        stringstream s(line);
        // this->rowCount += 1;
        rowCounter += 1;
        for (int blockCounter = 0; blockCounter < this->blocksPerRow; blockCounter++)
        {
            int columnsForThisBlock = this->maxColumnsPerBlock;
            if (blockCounter == this->blocksPerRow - 1)
                columnsForThisBlock = this->columnCount % this->maxColumnsPerBlock;
            if (columnsForThisBlock == 0)
                columnsForThisBlock = this->maxColumnsPerBlock;
            for (int columnCounter = 0; columnCounter < columnsForThisBlock; columnCounter++)
            {
                if (!getline(s, word, ','))
                    return false;
                // row[columnCounter] = stoi(word);
                logger.log(word);
                rowsInPage[0][columnCounter] = stoi(word);
            }
            // pageCounter++;
            // this->updateStatistics(rowsInPage[0]);
            bufferManager.writePage(this->matrixName, to_string(rowCounter) + "_" + to_string(blockCounter + 1), rowsInPage, columnsForThisBlock);
            // this->blockCount++;
            // this->rowsPerBlockCount.emplace_back(pageCounter);
        }
    }

    if (rowCounter == 0 || this->rowCount != this->columnCount)
        return false;
    // this->distinctValuesInColumns.clear();
    return true;
}

/**
 * @brief Function prints the first few rows of the table. If the table contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
/*void Matrix::print()
{
    logger.log("Matrix::print");
    uint count = min((long long)PRINT_COUNT, this->rowCount);

    //print headings
    this->writeRow(this->columns, cout);

    Cursor cursor(this->tableName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < count; rowCounter++)
    {
        row = cursor.getNext();
        this->writeRow(row, cout);
    }
    printRowCount(this->rowCount);
}*/

/**
 * @brief This function returns one row of the table using the cursor object. It
 * returns an empty row is all rows have been read.
 *
 * @param cursor 
 * @return vector<int> 
 */
/*void Matrix::getNextPage(Cursor *cursor)
{
    logger.log("Matrix::getNext");

    if (cursor->pageIndex < this->blockCount - 1)
    {
        cursor->nextPage(cursor->pageIndex + 1);
    }
}*/

/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Matrix::makePermanent()
{
    logger.log("Matrix::makePermanent");
    // if (!this->isPermanent())
    bufferManager.deleteFile(this->sourceFileName);
    // string newSourceFile = "../data/" + this->tableName + ".csv";
    // string newSourceFile = this->sourceFileName;
    // ofstream fout(newSourceFile, ios::out);

    //print headings
    // this->writeRow(this->columns, fout);

    // Cursor cursor(this->tableName, 0);
    // vector<int> row;
    // for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    // {
    // row = cursor.getNext();
    // this->writeRow(row, fout);
    // }
    this->isFirstChunk = true;
    this->isLastChunk = false;
    for (int rowCounter = 1; rowCounter <= this->rowCount; rowCounter++)
    {
        this->isFirstChunk = true;
        for (int blockCounter = 1; blockCounter <= this->blocksPerRow; blockCounter++)
        {
            if (blockCounter == this->blocksPerRow)
                this->isLastChunk = true;
            vector<int> r = Page(this->matrixName, to_string(rowCounter) + "_" + to_string(blockCounter)).getRow(0);
            this->writeRow(r);
            this->isFirstChunk = false;
        }
        this->isLastChunk = false;
    }
    // fout.close();
}

/**
 * @brief The unload function removes the table from the database by deleting
 * all temporary files created as part of this table
 *
 */
/*void Matrix::unload()
{
    logger.log("Matrix::~unload");
    for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
        bufferManager.deleteFile(this->tableName, pageCounter);
    if (!isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
}*/

/**
 * @brief Function that returns a cursor that reads rows from this table
 * 
 * @return Cursor 
 */
/*Cursor Matrix::getCursor()
{
    logger.log("Matrix::getCursor");
    Cursor cursor(this->tableName, 0);
    return cursor;
}*/
