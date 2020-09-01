# MATRIX TRANSPOSE
The task was to implement LOAD MATRIX ,EXPORT MATRIX & TRANSPOSE commands in the given SimpleRA for N X N Matrix.
 
## LOAD MATRIX [MATRIX_NAME]
*Blocking Procedure* : 
As the elements in the row can be of order 10^9, we divided each row of the row into small blocks in unspanned manner. In each block, we are storing chunks of row instead of chunks of matrix.

*Naming Procedure* :
For handling Matrix operations, we decided to create a separate folder named *Matrix* which would hold the necessary blocks named in the following format : **matrixname_Block_rownumber_blocknumber**
This was done to avoid naming collisions with relations.

To handle the command , we added specific cases for our matrix. Along the lines of table.cpp,tableCatalogue.cpp, we created matrx.cpp, matrixCatalogue.cpp etc. blockify() was implemented to handle block segregation of columns.

## TRANSPOSE [MATRIX_NAME]
Since, the requirement was to perform transpose operation inplace, we devised a formula to calculate the block number using row number & column number.
This was done by traversing the matrix in upper-triangular method and swapping the corresponding element. along the lines of  *swap(a[i][j],a[j][i])*.

## EXPORT MATRIX [MATRIX_NAME]

Since we didn't want to alter the export strategy for relations, we went for this option.( EXPORT MATRIX [MATRIX_NAME]).  The blocks of matrices were read one by one and written back to matrix_name.csv format.
