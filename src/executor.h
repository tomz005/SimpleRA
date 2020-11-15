#include "semanticParser.h"

void executeCommand();

void executeALTER();
void executeBULKINSERT();
void executeCLEAR();
void executeCROSS();
void executeDISTINCT();
void executeDELETE();
void executeEXPORT();
void executeINDEX();
void executeINSERT();
void executeJOIN();
void executeLIST();
void executeLOAD();
void executePRINT();
void executePROJECTION();
void executeRENAME();
void executeSELECTION();
void executeSORT();
void executeSOURCE();
void executeTRANSPOSE();

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator);
void printRowCount(int rowCount);