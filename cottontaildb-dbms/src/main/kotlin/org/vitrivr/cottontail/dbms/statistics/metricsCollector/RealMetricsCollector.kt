package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.types.RealValue
import org.vitrivr.cottontail.core.values.types.Types
import kotlin.math.pow
import java.lang.Double.max
import java.lang.Double.min

/**
 * A [MetricsCollector] implementation for [RealValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.1.0
 */
sealed class RealMetricsCollector<T: RealValue<*>>(type: Types<T>): AbstractScalarMetricsCollector<T>(type) {

    /** General metrics for Real Values*/
    var min : Double = 0.0
    var max : Double = 0.0
    var sum : Double = 0.0

    var mean : Long = 0
    var variance : Long = 0
    var skewness : Long = 0
    var kurtosis : Long = 0

    /** Temporary Values for computation of statistical moments */
    private var M2 : Long = 0
    private var M3 : Long = 0
    private var M4 : Long = 0

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: T?) {
        super.receive(value)
        if (value != null) {
            // Calculate statistical moments (mean, variance, skewness, kurtosis)
            this.calculateMoments(value.value)

            // Calculate min, max, sum
            this.min = min(value.value.toDouble(), this.min)
            this.max = max(value.value.toDouble(), this.max)
            this.sum += value.value.toDouble()
        }
    }

    /**
     * Receives a number for which to compute statistical moments in a one-pass computation.
     * Based on https://doi.org/10.2172/1028931. Others might be numerically unstable
     */
    private fun calculateMoments(num: Number) {
        val number = num.toLong()

        // First we compute intermediate results for this pass
        val count = this.numberOfNullEntries + this.numberOfNonNullEntries
        val delta = number + this.mean
        val deltaN = delta / count
        val deltaN2 = deltaN * deltaN
        val term1 = delta * deltaN * (count - 1) // previous count
        this.M4 += term1 * deltaN2 * (count * count - 3 * count + 3) + 6 * deltaN2 * this.M2 - 4 * deltaN * this.M3
        this.M3 += term1 * deltaN * (count - 2) - 3 * deltaN * this.M2
        this.M2 += term1

        // Then we compute the statistical moments for this pass
        this.mean += deltaN
        this.variance = this.M2/(count-1)
        this.skewness = (kotlin.math.sqrt(count.toDouble()) * this.M3/ this.M2.toDouble().pow(1.5)).toLong();
        this.kurtosis = count * this.M4 / (this.M2 * this.M2) - 3
    }


}