package org.vitrivr.cottontail.dbms.index.pq.quantizer

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.xerial.snappy.Snappy
import java.io.ByteArrayInputStream

/**
 * A serializable version of a [SingleStageQuantizer] pair for the [IVFPQIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SerializableMultiStageProductQuantizer(val coarse: Array<DoubleArray>, val fine: Array<Array<DoubleArray>>): IndexStructCatalogueEntry()  {

    /**
     * The [ComparableBinding] used to de-/serialize the [SerializableSingleStageProductQuantizer].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<SerializableMultiStageProductQuantizer> {
            val numberOfCoarseCentroids = IntegerBinding.readCompressed(stream)
            val numberOfFineSubspaces = IntegerBinding.readCompressed(stream)
            val numberOfFineCentroids = IntegerBinding.readCompressed(stream)

            val coarse = Array(numberOfCoarseCentroids) {
                val bytes = stream.readNBytes(IntegerBinding.readCompressed(stream))
                Snappy.uncompressDoubleArray(bytes)
            }
            val fine = Array(numberOfFineSubspaces) {
                Array(numberOfFineCentroids) {
                    val bytes = stream.readNBytes(IntegerBinding.readCompressed(stream))
                    Snappy.uncompressDoubleArray(bytes)
                }
            }

            return SerializableMultiStageProductQuantizer(coarse, fine)
        }

        override fun writeObject(output: LightOutputStream, `object`: Comparable<SerializableMultiStageProductQuantizer>) {
            require(`object` is SerializableMultiStageProductQuantizer) { "SerializableProductQuantizer.Binding can only be used to serialize instances of SerializableProductQuantizer." }
            IntegerBinding.writeCompressed(output, `object`.numberOfCoarseCentroids)
            IntegerBinding.writeCompressed(output, `object`.numberOfFineSubspaces)
            IntegerBinding.writeCompressed(output, `object`.numberOfFineCentroids)

            /* Serialize coarse centroids. */
            for (co in `object`.coarse) {
                val compressed = Snappy.compress(co)
                IntegerBinding.writeCompressed(output, compressed.size)
                output.write(compressed)
            }

            /* Serialize fine centroids. */
            for (cb in `object`.fine) {
                for (co in cb) {
                    val compressed = Snappy.compress(co)
                    IntegerBinding.writeCompressed(output, compressed.size)
                    output.write(compressed)
                }
            }
        }
    }

    /** The number of caorse subspaces as defined in this [SerializableMultiStageProductQuantizer]. */
    val numberOfCoarseCentroids
        get() = this.coarse.size

    /** The number of fine subspaces defined in this [SerializableMultiStageProductQuantizer]. */
    val numberOfFineSubspaces
        get() = this.fine.size

    /** The number of fine centroids per subspace. */
    val numberOfFineCentroids
        get() = this.fine[0].size

    /** The number of dimensions per subspace. */
    val dimensionsPerSubspace
        get() = this.fine[0][0].size

    /**
     * Converts this [SerializableSingleStageProductQuantizer] to a [SingleStageQuantizer].
     *
     * @param distance The [VectorDistance] to prepare the [SingleStageQuantizer] for.
     * @return New [SingleStageQuantizer] instance
     */
    fun toProductQuantizer(distance: VectorDistance<*>): MultiStageQuantizer {
        val coarse = PQCodebook(distance, Array(this.numberOfCoarseCentroids) { j ->
            when (distance.type) {
                is Types.DoubleVector -> DoubleVectorValue(this.coarse[j])
                is Types.FloatVector -> FloatVectorValue(this.coarse[j])
                is Types.LongVector -> LongVectorValue(this.coarse[j])
                is Types.IntVector -> IntVectorValue(this.coarse[j])
                else -> throw IllegalArgumentException("Reconstruction of product quantizer not possible; type ${distance.type} not supported.")
            }
        })
        val reshaped = distance.copy(this.dimensionsPerSubspace)
        val fine = Array(this.numberOfFineSubspaces) { i ->
            PQCodebook(reshaped, Array(this.numberOfFineCentroids) { j ->
                when (distance.type) {
                    is Types.DoubleVector -> DoubleVectorValue(this.fine[i][j])
                    is Types.FloatVector -> FloatVectorValue(this.fine[i][j])
                    is Types.LongVector -> LongVectorValue(this.fine[i][j])
                    is Types.IntVector -> IntVectorValue(this.fine[i][j])
                    else -> throw IllegalArgumentException("Reconstruction of product quantizer not possible; type ${distance.type} not supported.")
                }
            })
        }
        return MultiStageQuantizer(coarse, fine)
    }

    /**
     * Trivial comparator between two [IndexStructCatalogueEntry]. Proably never used.
     *
     * @param other The other [IndexStructCatalogueEntry] to compare this [IndexStructCatalogueEntry] to.
     * @return -1, 0 or 1
     */
    override fun compareTo(other: IndexStructCatalogueEntry): Int {
        if (other !is SerializableMultiStageProductQuantizer) return -1
        for ((i, cb) in this.coarse.withIndex()) {
            for ((j, d) in cb.withIndex()) {
                val compare = d.compareTo(other.coarse[i][j])
                if (compare != 0) return compare
            }
        }
        return 0
    }
}