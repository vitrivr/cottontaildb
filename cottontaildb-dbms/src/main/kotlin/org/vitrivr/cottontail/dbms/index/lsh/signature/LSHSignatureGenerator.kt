package org.vitrivr.cottontail.dbms.index.lsh.signature

import org.vitrivr.cottontail.core.types.VectorValue

/**
 * A helper class that generates a [LSHSignature] from a [VectorValue] after training.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface LSHSignatureGenerator {
    /**
     * Trains this [LSHSignatureGenerator] with the given [Iterator] of [VectorValue]s.
     *
     * @param vectors The [Sequence] of [VectorValue]s to train this [LSHSignatureGenerator] with.
     */
    fun train(vectors: Sequence<VectorValue<*>>)

    /**
     * Generates and returns a [LSHSignature] for the given [VectorValue].
     *
     * @param vector The [VectorValue] to generate [LSHSignature] for.
     * @return Resulting [LSHSignature].
     */
    fun generate(vector: VectorValue<*>): LSHSignature
}