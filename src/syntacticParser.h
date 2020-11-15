#include "tableCatalogue.h"
// #include "matrixCatalogue.h"

using namespace std;
class Matrix
{
    // vector<unordered_set<int>> distinctValuesInColumns;

public:
    string sourceFileName = "";
    string matrixName = "";
    // vector<string> columns;
    // vector<uint> distinctValuesPerColumnCount;
    uint columnCount = 0;
    long long int rowCount = 0;
    uint blockCount = 0;
    uint maxColumnsPerBlock = 0;
    vector<uint> rowsPerBlockCount;
    uint blocksPerRow = 0;
    // bool indexed = false;
    // string indexedColumn = "";
    // MatrixIndexingStrategy indexingStrategy = NOTHING;
    bool isLastChunk = false;
    bool isFirstChunk = true;

    bool extractColumnNames(string firstLine);
    bool blockify();
    // void updateStatistics(vector<int> row);
    Matrix();
    Matrix(string matrixName);
    // Table(string tableName, vector<string> columns);
    bool load();
    // bool isColumn(string columnName);
    // void renameColumn(string fromColumnName, string toColumnName);
    // void print();
    void makePermanent();
    // bool isPermanent();
    // void getNextPage(Cursor *cursor);
    // Cursor getCursor();
    // int getColumnIndex(string columnName);
    // void unload();

    /**
 * @brief Static function that takes a vector of valued and prints them out in a
 * comma seperated format.
 *
 * @tparam T current usaages include int and string
 * @param row 
 */
    template <typename T>
    void writeRow(vector<T> row, ostream &fout)
    {
        logger.log("Matrix::printRow");
        for (int columnCounter = 0; columnCounter < row.size(); columnCounter++)
        {
            if (columnCounter != 0 && this->isFirstChunk)
                fout << ", ";
            fout << row[columnCounter];
        }
        if (this->isLastChunk)
            fout << endl;
    }

    /**
 * @brief Static function that takes a vector of valued and prints them out in a
 * comma seperated format.
 *
 * @tparam T current usaages include int and string
 * @param row 
 */
    template <typename T>
    void writeRow(vector<T> row)
    {
        logger.log("Matrix::printRow");
        ofstream fout(this->sourceFileName, ios::app);
        this->writeRow(row, fout);
        fout.close();
    }
};

class MatrixCatalogue
{

    unordered_map<string, Matrix *> matrices;

public:
    MatrixCatalogue() {}
    void insertMatrix(Matrix *matrix);
    void deleteMatrix(string matrixName);
    Matrix *getMatrix(string matrixName);
    bool isMatrix(string matrixName);
    // bool isColumnFromTable(string columnName, string matrixName);
    void print();
    ~MatrixCatalogue();
};

enum QueryType
{
    ALTER,
    BULKINSERT,
    CLEAR,
    CROSS,
    DELETE,
    DISTINCT,
    EXPORT,
    INDEX,
    INSERT,
    JOIN,
    LIST,
    LOAD,
    PRINT,
    PROJECTION,
    RENAME,
    TRANSPOSE,
    SELECTION,
    SORT,
    SOURCE,
    UNDETERMINED
};

enum BinaryOperator
{
    LESS_THAN,
    GREATER_THAN,
    LEQ,
    GEQ,
    EQUAL,
    NOT_EQUAL,
    NO_BINOP_CLAUSE
};

enum SortingStrategy
{
    ASC,
    DESC,
    NO_SORT_CLAUSE
};

enum SelectType
{
    COLUMN,
    INT_LITERAL,
    NO_SELECT_CLAUSE
};

class ParsedQuery
{

public:
    QueryType queryType = UNDETERMINED;

    string alterRelationName = "";
    string alterColumnName = "";
    string alterMethod = "";

    string bulkinsertRelationName = "";
    string bulkinsertResultantRelationName = "";
    Table *bulkinsertsourceTable;

    string clearRelationName = "";

    string crossResultRelationName = "";
    string crossFirstRelationName = "";
    string crossSecondRelationName = "";

    string distinctResultRelationName = "";
    string distinctRelationName = "";

    string exportRelationName = "";
    string exportMatrixName = "";

    IndexingStrategy indexingStrategy = NOTHING;
    string indexColumnName = "";
    string indexRelationName = "";
    int indexStrategyCount = 1;

    string insertRelationName = "";
    vector<int> insertVector;

    BinaryOperator joinBinaryOperator = NO_BINOP_CLAUSE;
    string joinResultRelationName = "";
    string joinFirstRelationName = "";
    string joinSecondRelationName = "";
    string joinFirstColumnName = "";
    string joinSecondColumnName = "";

    string loadRelationName = "";
    string loadMatrixName = "";

    string printRelationName = "";

    string projectionResultRelationName = "";
    vector<string> projectionColumnList;
    string projectionRelationName = "";

    string renameFromColumnName = "";
    string renameToColumnName = "";
    string renameRelationName = "";

    SelectType selectType = NO_SELECT_CLAUSE;
    BinaryOperator selectionBinaryOperator = NO_BINOP_CLAUSE;
    string selectionResultRelationName = "";
    string selectionRelationName = "";
    string selectionFirstColumnName = "";
    string selectionSecondColumnName = "";
    int selectionIntLiteral = 0;

    SortingStrategy sortingStrategy = NO_SORT_CLAUSE;
    string sortResultRelationName = "";
    string sortColumnName = "";
    string sortRelationName = "";
    int sortBufferSize = 0;

    string sourceFileName = "";

    ParsedQuery();
    void clear();
};

bool syntacticParse();
bool syntacticParseALTER();
bool syntacticParseBULKINSERT();
bool syntacticParseCLEAR();
bool syntacticParseCROSS();
bool syntacticParseDISTINCT();
bool syntacticParseDELETE();
bool syntacticParseEXPORT();
bool syntacticParseINDEX();
bool syntacticParseINSERT();
bool syntacticParseJOIN();
bool syntacticParseLIST();
bool syntacticParseLOAD();
bool syntacticParseTRANSPOSE();
bool syntacticParsePRINT();
bool syntacticParsePROJECTION();
bool syntacticParseRENAME();
bool syntacticParseSELECTION();
bool syntacticParseSORT();
bool syntacticParseSOURCE();

bool isFileExists(string tableName);
bool isQueryFile(string fileName);
