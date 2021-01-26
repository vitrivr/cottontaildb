package org.vitrivr.cottontail.database.index

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.gg.GGIndex
import org.vitrivr.cottontail.database.index.gg.GGIndexConfig
import org.vitrivr.cottontail.database.index.hash.NonUniqueHashIndex
import org.vitrivr.cottontail.database.index.hash.UniqueHashIndex
import org.vitrivr.cottontail.database.index.lsh.superbit.SuperBitLSHIndex
import org.vitrivr.cottontail.database.index.lsh.superbit.SuperBitLSHIndexConfig
import org.vitrivr.cottontail.database.index.lucene.LuceneIndex
import org.vitrivr.cottontail.database.index.pq.PQIndex
import org.vitrivr.cottontail.database.index.pq.PQIndexConfig
import org.vitrivr.cottontail.database.index.va.VAFIndex
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.VectorValue

enum class IndexType(val inexact: Boolean) {
    HASH_UQ(false), /* A hash based index with unique values. */

    HASH(false), /* A hash based index. */

    BTREE(false), /* A BTree based index. */

    LUCENE(false), /* A Lucene based index (fulltext search). */

    VAF(false), /* A VA file based index (for exact kNN lookup). */

    PQ(true), /* A product quantization based index (for approximate kNN lookup). */

    SH(true), /* A spectral hashing based index (for approximate kNN lookup). */

    LSH(true), /* A locality sensitive hashing based index for approximate kNN lookup with Lp distance. */

    LSH_SB(true), /* A super bit locality sensitive hashing based index for approximate kNN lookup with cosine distance. */

    GG(true);

    /**
     * Opens an index of this [IndexType] using the given name and [Entity].
     *
     * @param name [Name.IndexName] of the [Index]
     * @param entity The [Entity] the desired [Index] belongs to.
     * @param columns The [ColumnDef] for which to open the [Index]
     */
    fun open(name: Name.IndexName, entity: Entity, columns: Array<ColumnDef<*>>): Index = when (this) {
        HASH_UQ -> UniqueHashIndex(name, entity, columns)
        HASH -> NonUniqueHashIndex(name, entity, columns)
        LUCENE -> LuceneIndex(name, entity, columns)
        LSH_SB -> SuperBitLSHIndex<VectorValue<*>>(name, entity, columns)
        VAF -> VAFIndex(name, entity, columns)
        PQ -> PQIndex(name, entity, columns)
        GG -> GGIndex(name, entity, columns)
        else -> throw NotImplementedError("Index of type $this is not implemented.")
    }

    /**
     * Creates an index of this [IndexType] using the given name and [Entity].
     *
     * @param name [Name.IndexName] of the [Index]
     * @param entity The [Entity] the desired [Index] belongs to.
     * @param columns The [ColumnDef] for which to create the [Index]
     * @param params Additions configuration params.
     */
    fun create(name: Name.IndexName, entity: Entity, columns: Array<ColumnDef<*>>, params: Map<String, String> = emptyMap()) = when (this) {
        HASH_UQ -> UniqueHashIndex(name, entity, columns)
        HASH -> NonUniqueHashIndex(name, entity, columns)
        LUCENE -> LuceneIndex(name, entity, columns)
        LSH_SB -> SuperBitLSHIndex<VectorValue<*>>(name, entity, columns, SuperBitLSHIndexConfig.fromParamMap(params))
        VAF -> VAFIndex(name, entity, columns)
        PQ -> PQIndex(name, entity, columns, PQIndexConfig.fromParamMap(params))
        GG -> GGIndex(name, entity, columns, GGIndexConfig.fromParamsMap(params))
        else -> throw NotImplementedError("Index of type $this is not implemented.")
    }
}