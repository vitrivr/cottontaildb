package org.vitrivr.cottontail.database.index.pq

import org.apache.commons.math3.complex.Complex
import org.apache.commons.math3.linear.BlockFieldMatrix
import org.apache.commons.math3.linear.FieldLUDecomposition
import org.apache.commons.math3.linear.FieldMatrix
import org.apache.commons.math3.linear.FieldMatrixChangingVisitor
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.model.values.Complex32VectorValue
import org.vitrivr.cottontail.model.values.Complex64Value
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.types.ComplexVectorValue
import kotlin.reflect.KClass

/**
 *
 * estimate non centered cov matrix in real case via Q_XX = M_X * M_X^T
 * we estimate cov matrix in complex case as Q_XX = 1/n * M_X * M_X^H where ()^H means conjugate transpose
 * todo: * compare to matlab
 *       * check if we could not change to row-vector order to have the same convention
 *         as math commons library. Math reference (https://en.wikipedia.org/wiki/Covariance_matrix#Estimation)
 *         and paragraph there about complex covariances is with column vectors.
 *         swapping rows <-> cols is transposing, and in real case we could just change the order of
 *         multiplication M*M^T->M^T*M to cancel the effect, but for complex we do the conjugate transpose
 *         and the effect doesn't generally cancel! Resolve this.
 * @param An array of [ComplexVectorValue]s, each being a column vector of the data matrix (vector components
 * are rows.
 * @return the complex covariance matrix as a [FieldMatrix]
 */
fun complexCovarianceMatrix(data: Array<out ComplexVectorValue<*>>): FieldMatrix<Complex> {
    val fieldDataMatrix = BlockFieldMatrix<Complex>(Array(data[0].logicalSize) { row ->
        Array(data.size) { col ->
            Complex(data[col][row].real.value.toDouble(), data[col][row].imaginary.value.toDouble())
        }
    })
    val fieldDataMatrixH = fieldDataMatrix.transpose()
    fieldDataMatrixH.walkInOptimizedOrder(object : FieldMatrixChangingVisitor<Complex> {
        override fun end() : Complex {return Complex.ZERO}
        override fun start(rows: Int, columns: Int, startRow: Int, endRow: Int, startColumn: Int, endColumn: Int) { return }
        override fun visit(row: Int, column: Int, value: Complex) = value.conjugate()
    })
    return fieldDataMatrix.multiply(fieldDataMatrixH).scalarMultiply(Complex(1.0 / data.size.toDouble(), 0.0))
}

fun invertComplexMatrix(matrix: FieldMatrix<Complex>): FieldMatrix<Complex> {
    val lud = FieldLUDecomposition(matrix)
    return lud.solver.inverse
}

/**
 * todo: * I'm sure this when construct can be made prettier instead of explicit type checking.
 *
 * @return a matrix as an array of column-vectors
 */
fun fieldMatrixToVectorArray(matrixCommons: FieldMatrix<Complex>, type: KClass<out ComplexVectorValue<*>>): Array<out ComplexVectorValue<*>> {
    // math commons stores as row vectors
    return Array(matrixCommons.columnDimension) { col ->
        when (type) {
            Complex32VectorValue::class -> {
                Complex32VectorValue(Array(matrixCommons.rowDimension) { row ->
                    Complex32Value(matrixCommons.data[row][col].real.toFloat(),
                            matrixCommons.data[row][col].imaginary.toFloat())
                })
            }
            Complex64VectorValue::class -> {
                Complex64VectorValue(Array(matrixCommons.rowDimension) { row ->
                    Complex64Value(matrixCommons.data[row][col].real,
                            matrixCommons.data[row][col].imaginary)
                })
            }
            else -> {
                error("Unsupported type $type")
            }
        }
    }
}


