package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.config.StatisticsConfig
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.statistics.metricsData.AbstractValueMetrics
import java.lang.Math.pow
import java.lang.Math.sqrt
import kotlin.math.pow

/**
 * A [MetricsCollector] for [VectorValue]s
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.0.0
 */
sealed class RealVectorMetricsCollector<T: RealVectorValue<*>>(type: Types<T>): AbstractVectorMetricsCollector<T>(type) {

    /** Local Metrics */
    var mean : Long = 0
    var variance : Long = 0
    var skewness : Long = 0
    var kurtosis : Long = 0

    /** Temporary variables */
    var sum : Long = 0 // already have that stored
    var sumSquares : Long  = 0
    var count : Long  = 0 // already have that stored

    /** Welford's online algorithm Varaibles */
    var M : Long = 0
    var M1 : Long = 0
    var M2 : Long = 0
    var M3 : Long = 0
    var M4 : Long = 0
    var S : Long = 0

    /**
     * // TODO this function is not done yet
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is LongValue) { // TODO adapt for all possible values not only LongValues

            // Welford's online algorithm to calculate variance
            //val oldM = this.M
            //this.M += (value.value - M) / count
            //this.S += (value.value - M) * (value.value - oldM)


            // Steps to calculate all moments in one-pass based on https://doi.org/10.2172/1028931
            this.count += 1
            var delta = value.value - this.M1;
            var delta_n = delta / this.count;
            var delta_n2 = delta_n * delta_n;
            var term1 = delta * delta_n * (this.count -1); // use prev. count
            this.M1 += delta_n;
            this.M4 += term1 * delta_n2 * (count*count - 3*count + 3) + 6 * delta_n2 * M2 - 4 * delta_n * M3;
            this.M3 += term1 * delta_n * (count - 2) - 3 * delta_n * M2;
            this.M2 += term1;


        }
    }

    fun calculateMoments() : Unit {
        //this.mean = sum / count
        //this.variance = this.S / (count - 1) // based on Welford's online algorithm

        // based on https://doi.org/10.2172/1028931
        // Others were numerical unstable
        this.mean = this.M1
        this.variance = this.M2/(count-1)
        this.skewness = (kotlin.math.sqrt(count.toDouble()) * this.M3/ this.M2.toDouble().pow(1.5)).toLong();
        this.kurtosis = (count * this.M4 / (this.M2 * this.M2) - 3.0).toLong();

    }

}