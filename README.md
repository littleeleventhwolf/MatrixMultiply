# MatrixMultiply
Use MapReduce to compute the product of two matrix.

# Illustration
![](images/matrixA)
pic1: Matrix A (4x5)

![](images/matrixB)
pic2: Transpodition of Matrix B (5x5)

We get the product of matrix A and matrix B by computing the A\*(transopisition of B).

In map stage: if file name is 'matrixA', the key C\_i\_j represents the row i, the value is a series of column-value splitted by colon; if file name is 'matrixB', the key C\_i\_j represents the column j, the value is a series of row-value splitted by colon.
In reduce stage: if two keys are same, split the value into an array, and multiply the values which have the same index in array, then add all the middle calculated value.

![](images/result.png)
pic3: The Result File
