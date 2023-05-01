package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.types.RealValue
import org.vitrivr.cottontail.core.values.types.Types
import java.lang.Double.max
import java.lang.Double.min
import kotlin.math.pow

/**
 * A [MetricsCollector] implementation for [RealValue]s.
 *
 * The calculation of the statistical moments mean, variance, skewness, and kurtosis is based on the approach outlined in
 * the paper "Formulas for robust, one-pass parallel computation of covariances and arbitrary-order statistical moments."
 * (https://doi.org/10.2172/1028931). Others approaches might be numerically unstable
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.1.0
 */
sealed class RealMetricsCollector<T: RealValue<*>>(type: Types<T>, config: MetricsConfig): AbstractScalarMetricsCollector<T>(type, config) {

    /** General metrics for Real Values*/
    var min : Double = Double.MAX_VALUE
    var max : Double = Double.MIN_VALUE
    var sum : Double = 0.0

    var mean : Double = 0.0
    var variance : Double = 0.0
    var skewness : Double = 0.0
    var kurtosis : Double = 0.0

    /** Temporary Values for computation of statistical moments */
    private var d_count : Int = 0 // Number of entries.
    private var d_sum : Double = 0.0 // Sum of entries.
    private var d_mean : Double = 0.0 // Mean of entries.
    private var d_M2 : Double = 0.0 // 2nd moment, for variance.
    private var d_M3 : Double = 0.0 // 3rd moment, for skew
    private var d_M4 : Double= 0.0 // 4th moment, for kurtosis

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: T?) {
        super.receive(value)
        if (value != null) {
            // Calculate statistical moments (mean, variance, skewness, kurtosis)
            this.processMoments(value.value)

            // Calculate min, max, sum
            this.min = min(value.value.toDouble(), this.min)
            this.max = max(value.value.toDouble(), this.max)
            this.sum += value.value.toDouble()
        }
    }

    /**
     * Receives a number for which to compute statistical moments in a one-pass computation.
     */
    private fun processMoments(num: Number) {
        val number = num.toLong()

        // Intermediary calculations for this pass
        val delta = number - this.d_mean
        val nm1 = this.d_count // before increasing count
        this.d_sum += number
        this.d_count++
        val n = this.d_count // after increasing count
        val n2 = n * n
        val deltaN = delta / n
        this.d_mean = this.d_sum / n
        val term1 = delta * deltaN * nm1
        val deltaN2 = deltaN * deltaN
        this.d_M4 += term1 * deltaN2 * (n2 - 3.0 * n + 3.0) + 6 * deltaN2 * this.d_M2 - 4.0 * deltaN * this.d_M3;
        this.d_M3 += term1 * deltaN * (n - 2.0) - 3.0 * deltaN * this.d_M2;
        this.d_M2 += term1;

        // Translate to statistical moments
        // Mean
        this.mean = if (1 <= this.d_count) this.d_mean else 0.0

        // Variance
        this.variance = if (2 <= this.d_count) this.d_M2 / (this.d_count - 1) else 0.0

        // Calculate Skewness. Can only be calculated when some conditions are met (e.g., dataset large enough)
        if (3 <= this.d_count && 0.0 != this.d_M2) {
            val d_count_double = this.d_count.toDouble()
            this.skewness =  ( kotlin.math.sqrt(d_count_double-1) ) * d_count_double / (d_count_double - 2) * this.d_M3 / this.d_M2.pow(1.5)
        }

        // Calculate Kurtosis. Can only be calculated when some conditions are met (e.g., dataset large enough)
        if ((4 <= this.d_count) && (this.d_M2 != 0.0)) {
            val n1 = (this.d_count - 1.0)
            val n2n3 = (n - 2.0) * (n - 3.0)
            this.kurtosis = n * (n + 1.0) * n1 / n2n3 * this.d_M4 / this.d_M2 / this.d_M2 - 3.0 * n1 * n1 / n2n3
        } else {
            this.kurtosis = 0.0
        }
    }

}