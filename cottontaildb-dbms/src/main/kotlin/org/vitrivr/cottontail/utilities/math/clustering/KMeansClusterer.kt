package org.vitrivr.cottontail.utilities.math.clustering

import org.apache.commons.math3.exception.ConvergenceException
import org.apache.commons.math3.exception.util.LocalizedFormats
import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.stat.descriptive.moment.Variance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.types.VectorValue
import java.util.*
import kotlin.math.pow

/**
 * A [Clusterer] that uses the k-means++ algorithm.
 *
 * This implementation is an adaption of the Apache Commons Math [org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class KMeansClusterer(val k: Int, override val distance: VectorDistance<*>, private val random: RandomGenerator, private val maxIterations: Int = MAX_ITERATIONS): Clusterer {

    companion object {
        /** Maximum number of iterations to use for k means clustering. */
        private const val MAX_ITERATIONS = 250
    }

    /**
     *
     */
    override fun cluster(points: List<VectorValue<*>>): List<Cluster> {


        // number of clusters has to be smaller or equal the number of data points
        require(points.size >= k) { "Number of points must be larger than the desired number of cluster centers!" }
        var clusters: List<KMeansCluster> = chooseInitialCenters(points)

        /* Create an array containing the latest assignment of a point to a cluster. */
        val assignments = IntArray(points.size)
        assignPointsToClusters(clusters, points, assignments)

        /* Iterate through updating the centers until we're done. */
        val max = if (maxIterations < 0) Int.MAX_VALUE else maxIterations
        for (count in 0 until max) {
            var emptyCluster = false
            val newClusters: MutableList<KMeansCluster> = ArrayList<KMeansCluster>(this.k)
            for (cluster in clusters) {
                val newCenter: VectorValue<*> = if (cluster.points.isEmpty()) {
                    emptyCluster = true
                    this.getNewCenter(clusters)
                } else {
                    centroidOf(cluster.points)
                }
                newClusters.add(KMeansCluster(newCenter))
            }
            val changes: Int = assignPointsToClusters(newClusters, points, assignments)
            clusters = newClusters

            /* If there were no more changes in the point-to-cluster assignmen, and there are no empty clusters left, return the current clusters. */
            if (changes == 0 && !emptyCluster) {
                return clusters
            }
        }
        return clusters
    }


    /**
     * Adds the given points to the closest [Cluster].
     *
     * @param clusters the [Cluster]s to add the points to
     * @param points the points to add to the given [Cluster]s
     * @param assignments points assignments to clusters
     * @return the number of points assigned to different clusters as the iteration before
     */
    private fun assignPointsToClusters(clusters: List<KMeansCluster>, points: List<VectorValue<*>>, assignments: IntArray): Int {
        var assignedDifferently = 0
        for ((pointIndex, p) in points.withIndex()) {
            val clusterIndex: Int = getNearestCluster(clusters, p)
            if (clusterIndex != assignments[pointIndex]) {
                assignedDifferently++
            }
            val cluster = clusters[clusterIndex]
            cluster.addPoint(p)
            assignments[pointIndex] = clusterIndex
        }
        return assignedDifferently
    }

    /**
     * Returns the nearest Cluster to the given point.
     *
     * @param clusters The list of [KMeansCluster]s.
     * @param point The
     * @return The index of the nearest [KMeansCluster] to the given point
     */
    private fun getNearestCluster(clusters: List<KMeansCluster>, point: VectorValue<*>): Int {
        var minDistance = Double.MAX_VALUE
        var minCluster = 0
        for ((clusterIndex, c) in clusters.withIndex()) {
            val distance: Double = c.distance(point).value
            if (distance < minDistance) {
                minDistance = distance
                minCluster = clusterIndex
            }
        }
        return minCluster
    }

    /**
     *
     */
    private fun chooseInitialCenters(points: List<VectorValue<*>>): MutableList<KMeansCluster> {

        // Set the corresponding element in this array to indicate when
        // elements of pointList are no longer available.
        val taken = BooleanArray(points.size)

        // The resulting list of initial centers.
        val resultSet: MutableList<KMeansCluster> = ArrayList(this.k)

        // Choose one center uniformly at random from among the data points.
        val firstPointIndex = this.random.nextInt(points.size)
        resultSet.add(KMeansCluster(points[firstPointIndex]))

        // Must mark it as taken
        taken[firstPointIndex] = true

        /* To keep track of the minimum distance squared of elements of pointList to elements of resultSet. */
        val minDistSquared = DoubleArray(points.size) {
            this.distance(points[firstPointIndex], points[it])!!.value.pow(2.0)
        }

        /* Initialize the elements. Since the only point in resultSet is firstPoint, this is very easy. */
        while (resultSet.size < this.k) {

            /* Sum up the squared distances for the points in pointList not already taken. */
            val distSqSum = minDistSquared.filterIndexed { index, _ -> !taken[index] }.sum()

            /* Add one new data point as a center. Each point x is chosen with probability proportional to D(x)2. */
            val r = this.random.nextDouble() * distSqSum

            // The index of the next point to be added to the resultSet.
            var nextPointIndex = -1

            /* Sum through the squared min distances again, stopping when sum >= r. */
            var sum = 0.0
            for (i in points.indices) {
                if (!taken[i]) {
                    sum += minDistSquared[i]
                    if (sum >= r) {
                        nextPointIndex = i
                        break
                    }
                }
            }

            /* If it's not set to >= 0, the point wasn't found in the previous for loop, probably because distances are extremely small. Just pick the last available point. */
            if (nextPointIndex == -1) {
                for (i in points.size - 1 downTo 0) {
                    if (!taken[i]) {
                        nextPointIndex = i
                        break
                    }
                }
            }

            // We found one.
            if (nextPointIndex >= 0) {
                val p = points[nextPointIndex]
                resultSet.add(KMeansCluster(p))

                // Mark it as taken.
                taken[nextPointIndex] = true
                if (resultSet.size < k) {
                    /* Now update elements of minDistSquared.  We only have to compute the distance to the new center to do this. */
                    for (j in points.indices) {
                        if (!taken[j]) {
                            val d2: Double = distance(p, points[j])!!.value.pow(2.0)
                            if (d2 < minDistSquared[j]) {
                                minDistSquared[j] = d2
                            }
                        }
                    }
                }
            } else {
                // None found --
                // Break from the while loop to prevent
                // an infinite loop.
                break
            }
        }
        return resultSet
    }

    /**
     *
     */
    private fun getNewCenter(clusters: List<KMeansCluster>): VectorValue<*> {
        var maxVariance = Double.NEGATIVE_INFINITY
        var selected: Cluster? = null
        for (cluster in clusters) {
            if (cluster.points.isNotEmpty()) {

                // compute the distance variance of the current cluster
                val center = cluster.center
                val stat = Variance()
                for (point in cluster.points) {
                    stat.increment(this@KMeansClusterer.distance(point, center)!!.value)
                }
                val variance = stat.result

                // select the cluster with the largest variance
                if (variance > maxVariance) {
                    maxVariance = variance
                    selected = cluster
                }
            }
        }

        /** Did we find at least one non-empty cluster?. */
        if (selected == null) {
            throw ConvergenceException(LocalizedFormats.EMPTY_CLUSTER_IN_K_MEANS)
        }

        /* Extract a random point from the cluster. */
        val selectedPoints = (selected.points as LinkedList)
        return selectedPoints.removeAt(this.random.nextInt(selectedPoints.size))
    }

    /**
     * Computes the centroid for a set of points.
     *
     * @param points The list of points [VectorValue] to calculate the centroid of.
     */
    private fun centroidOf(points: List<VectorValue<*>>): VectorValue<*> {
        var centroid: VectorValue<*> = points.first()
        for (i in 1 until points.size) {
            centroid += points[i]
        }
        centroid /= IntValue(points.size)
        return centroid
    }

    /**
     * A [Cluster] that is generated as a result of this [KMeansCluster].
     */
    inner class KMeansCluster(override val center: VectorValue<*>): Cluster {

        /** The [List] of [VectorValue] held by this [KMeansCluster]. */
        override val points: List<VectorValue<*>> = LinkedList()

        /**
         * Adds a [VectorValue] to this [KMeansCluster].
         *
         * @param point The [VectorValue] to add.
         */
        override fun addPoint(point: VectorValue<*>) {
            (this.points as LinkedList).add(point)
        }

        /**
         * Calculates the distance between this [KMeansCluster] and the given [VectorValue].
         *
         * @param vector The [VectorValue].
         * @return The calculated distance.
         */
        fun distance(vector: VectorValue<*>): DoubleValue = this@KMeansClusterer.distance(vector, this.center)!!
    }
}