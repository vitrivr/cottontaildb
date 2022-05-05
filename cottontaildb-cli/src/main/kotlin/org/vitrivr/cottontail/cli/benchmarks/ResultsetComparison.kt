package org.vitrivr.cottontail.cli.benchmarks

import org.vitrivr.cottontail.client.iterators.TupleIterator

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object ResultsetComparison {


    /**
     *
     */
    fun <T: Any> compare(baseline: List<T>, iterator: TupleIterator, columnIndex: Int = 0) : Pair<Double, Double> {
        var rankAccuracy = 0.0
        var overlapAccuracy = 0.0
        var i = 0
        for (t in iterator) {
            if (baseline[i++] == t[columnIndex]) rankAccuracy += 1.0
            if (baseline.contains(t[columnIndex])) overlapAccuracy += 1.0
        }
        return rankAccuracy / i to overlapAccuracy / i
    }
}