package org.vitrivr.cottontail.dbms.index.pq.quantizer

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.LongVectorValue
import org.xerial.snappy.Snappy

/**
 * A serializable version of a [SingleStageQuantizer].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class SerializableSingleStageProductQuantizer(val codebooks: Array<Array<DoubleArray>>) {

    companion object {
        val EMPTY = SerializableSingleStageProductQuantizer(emptyArray())
    }

    /**
     * The [ComparableBinding] used to de-/serialize the [SerializableSingleStageProductQuantizer].
     */
    object Binding {
        fun deserialize(iterable: ByteIterable): SerializableSingleStageProductQuantizer {
            val stream = ByteArraySizedInputStream(iterable.bytesUnsafe, 0, iterable.length)
            val numberOfSubspaces = IntegerBinding.readCompressed(stream)
            val numberOfCentroids = IntegerBinding.readCompressed(stream)
            return SerializableSingleStageProductQuantizer(Array(numberOfSubspaces) {
                Array(numberOfCentroids) {
                    val bytes = stream.readNBytes(IntegerBinding.readCompressed(stream))
                    Snappy.uncompressDoubleArray(bytes)
                }
            })
        }

        fun serialize(`object`: SerializableSingleStageProductQuantizer): ByteIterable {
            val output = LightOutputStream()
            IntegerBinding.writeCompressed(output, `object`.numberOfSubspaces)
            IntegerBinding.writeCompressed(output, `object`.numberOfCentroids)
            for (cb in `object`.codebooks) {
                for (co in cb) {
                    val compressed = Snappy.compress(co)
                    IntegerBinding.writeCompressed(output, compressed.size)
                    output.write(compressed)
                }
            }
            return output.asArrayByteIterable()
        }
    }

    /** The number of subspaces as defined in this [SerializableSingleStageProductQuantizer]. */
    val numberOfSubspaces
        get() = this.codebooks.size

    /** The number of centroids per subspace. */
    val numberOfCentroids
        get() = this.codebooks[0].size

    /** The number of dimensions per subspace. */
    val dimensionsPerSubspace
        get() = this.codebooks[0][0].size

    /**
     * Converts this [SerializableSingleStageProductQuantizer] to a [SingleStageQuantizer].
     *
     * @param distance The [VectorDistance] to prepare the [SingleStageQuantizer] for.
     * @return New [SingleStageQuantizer] instance
     */
    fun toProductQuantizer(distance: VectorDistance<*>): SingleStageQuantizer {
        val reshaped = distance.copy(this.dimensionsPerSubspace)
        val codebooks = Array(this.numberOfSubspaces) { i ->
            PQCodebook(reshaped, Array(this.numberOfCentroids) { j ->
                when (distance.type) {
                    is Types.DoubleVector -> DoubleVectorValue(this.codebooks[i][j])
                    is Types.FloatVector -> FloatVectorValue(this.codebooks[i][j])
                    is Types.LongVector -> LongVectorValue(this.codebooks[i][j])
                    is Types.IntVector -> IntVectorValue(this.codebooks[i][j])
                    else -> throw IllegalArgumentException("Reconstruction of product quantizer not possible; type ${distance.type} not supported.")
                }
            })
        }
        return SingleStageQuantizer(codebooks)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SerializableSingleStageProductQuantizer) return false
        if (!this.codebooks.contentDeepEquals(other.codebooks)) return false
        return true
    }

    override fun hashCode(): Int {
        return this.codebooks.contentDeepHashCode()
    }
}