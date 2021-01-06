package org.vitrivr.cottontail.testutils

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import org.vitrivr.cottontail.model.values.Complex64Value
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import java.io.File
import java.io.FileInputStream
import java.util.*
import java.util.zip.GZIPInputStream

fun sampleVectorsFromCsv(infile: String, gzip: Boolean, outfile: String, rng: Random, probability: Double = 1e-5) {
    println("Sampling vectors from $infile with probability of $probability. Writing to $outfile")
    val ips = if (gzip) GZIPInputStream(FileInputStream(infile)) else FileInputStream(infile)
    csvReader().open(ips) {
        csvWriter().open(outfile) {
            writeRow(readNext()!!)
            writeRows(readAllAsSequence().filter { rng.nextDouble() < probability }.toList())
        }
    }
}

fun getComplexVectorsFromFile(file: String, skipColsBeginning: Int = 1, numDim: Int): Array<Complex64VectorValue> {
    val f = File(file)
    if (!f.exists()) {
        sampleVectorsFromCsv("src/test/resources/complexVectors.csv", false, file, Random(1234), 2e-5)
    }
    val vecs = csvReader().open(file) {
        readAllAsSequence().drop(1).toList()
    }
    println("${vecs.size} vectors read.")
    val vectors = vecs.map { row ->
        val v = Complex64VectorValue((0 until numDim).map { Complex64Value(row[it + skipColsBeginning].toDouble(), row[it + skipColsBeginning + numDim].toDouble()) }.toTypedArray())
        if (v.norm2() > 0.0) v / v.norm2() else v
    }.toTypedArray()
    return vectors
}

fun getRandomRealVectors(rng: Random, numVecs: Int, numDim: Int): Array<DoubleVectorValue> {
    val vectors = Array(numVecs) {
        val v = DoubleVectorValue(DoubleArray(numDim) { rng.nextGaussian() })
        v / v.norm2()
    }
    return vectors
}

fun getRandomComplexVectors(rng: Random, numVecs: Int, numDim: Int): Array<Complex64VectorValue> {
    val vectors = Array(numVecs) {
        val v = Complex64VectorValue(DoubleArray(numDim * 2) { rng.nextGaussian() })
        v / v.norm2()
    }
    return vectors
}
