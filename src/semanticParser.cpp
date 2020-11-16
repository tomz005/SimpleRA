#include "global.h"

bool semanticParse()
{
    logger.log("semanticParse");
    switch (parsedQuery.queryType)
    {
    case ALTER:
        return semanticParseALTER();
    case BULKINSERT:
        return semanticParseBULKINSERT();
    case CLEAR:
        return semanticParseCLEAR();
    case CROSS:
        return semanticParseCROSS();
    case DISTINCT:
        return semanticParseDISTINCT();
    case EXPORT:
        return semanticParseEXPORT();
    case GROUPBY:
        return semanticParseGROUPBY();
    case INDEX:
        return semanticParseINDEX();
    case JOIN:
        return semanticParseJOIN();
    case LIST:
        return semanticParseLIST();
    case LOAD:
        return semanticParseLOAD();
    case PRINT:
        return semanticParsePRINT();
    case PROJECTION:
        return semanticParsePROJECTION();
    case RENAME:
        return semanticParseRENAME();
    case SELECTION:
        return semanticParseSELECTION();
    case SORT:
        return semanticParseSORT();
    case SOURCE:
        return semanticParseSOURCE();
    case TRANSPOSE:
        return semanticParseTRANSPOSE();
    case DELETE:
        return semanticParseDELETE();
    case INSERT:
        return semanticParseINSERT();
    default:
        cout << "SEMANTIC ERROR" << endl;
    }

    return false;
}