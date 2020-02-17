package ch.unibas.dmi.dbis.cottontail.database.index.va.vaplus

typealias Mark = Pair<Double, Double>

object MarksGenerator {

    /**
     * Get marks.
     */
    fun getMarks(data: Array<DoubleArray>, numberOfMarks: List<Int>): Array<List<Mark>>? {
        val EPSILON = 10E-9
        val init = createEquiDistanceMarks(data, numberOfMarks).map {
            // TODO
            //x => x ++ Seq(Vector.conv_double2vb(x.last + EPSILON))
        }
        numberOfMarks.indices.map { it ->
            var marks = init[it]
            var delta: Double
            var deltaBar = Double.MAX_VALUE
            do {
                // k-means
                delta = deltaBar
                // TODO
                //val points = samples.map(_.ap_indexable.apply(it))
                //val rjs = marks.sliding(2).toList.map {
                //    list => {
                //    val filteredPoints = points.filter(p => p >= list (0) && p < list(1))
                //    if (filteredPoints.isEmpty()) {
                //        list.toSeq
                //    } else {
                //        filteredPoints.toSeq
                //    }
                //}
                //}.map(fps =>(1.0 / fps.length) * fps.sum).map(Vector.conv_double2vb(_))
                //val cjs = Seq(rjs.head)++ rjs . sliding (2).map(x => x . sum / x . length).toList
                //marks = cjs
                //deltaBar = marks.sliding(2).map { list => points.filter(p => p >= list(0) && p < list(1)) }.toList.zip(rjs).map { case(fps, rj) => fps.map(fp => (fp-rj) * (fp-rj)).sum }.sum
            } while (deltaBar / delta < 0.999)
            //return marks
        }
        return null
    }

    /**
     * Create marks per dimension (equally spaced).
     */
    private fun createEquiDistanceMarks(data: Array<DoubleArray>, maxMarks: List<Int>): Array<List<Mark>> {
        val min = getMin(data)
        val max = getMax(data)
        return Array(min.size) { it ->
            List(maxMarks[it] - 1) {
                val space = (max[it] - min[it]) / (maxMarks[it] - 1)
                Mark(it * space, (it * space) + space)
            }
        }
    }

    /**
     * Get vector ([DoubleArray]) which values are minimal.
     */
    private fun getMin(data: Array<DoubleArray>): DoubleArray = data.fold(DoubleArray(data.first().size) { Double.MAX_VALUE }) { a, b ->
        DoubleArray(a.size) { minOf(a[it], b[it]) }
    }

    /**
     * Get vector ([DoubleArray]) which values are maximal.
     */
    private fun getMax(data: Array<DoubleArray>): DoubleArray = data.fold(DoubleArray(data.first().size) { Double.MIN_VALUE }) { a, b ->
        DoubleArray(a.size) { maxOf(a[it], b[it]) }
    }

}
