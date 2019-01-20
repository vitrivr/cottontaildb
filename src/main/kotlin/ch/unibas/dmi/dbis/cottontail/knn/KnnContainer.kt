package ch.unibas.dmi.dbis.cottontail.knn

import ch.unibas.dmi.dbis.cottontail.knn.metrics.DistanceFunction

import java.util.*

/**
 *
 */
class KnnContainer(private val n: Int, private val query: FloatArray, private val distance: DistanceFunction) {

    /**  */
    val knn = TreeSet<Pair<Long,Double>> { o1, o2 -> o1.second.compareTo(o2.second) }

    /**
     *
     * @param id
     * @param vector
     */
    fun add(tupleId: Long, feature: FloatArray) {
        if (feature.size != this.query.size) throw IllegalArgumentException("The query vector and the provided vector do not have the same size.")
        val dist = this.distance(this.query, feature)
        if (this.knn.size < this.n) {
            this.knn.add(Pair(tupleId, dist))
        } else if (dist < this.knn.last().second) {
            this.knn.add(Pair(tupleId, dist))
            this.knn.pollLast()
        }
    }

    /**
     *
     * @return
     */
    fun knn(): List<Pair<Long, Double>> {
        return this.knn.toList()
    }
}
