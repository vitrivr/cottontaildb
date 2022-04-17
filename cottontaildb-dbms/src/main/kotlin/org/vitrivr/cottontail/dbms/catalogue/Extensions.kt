package org.vitrivr.cottontail.dbms.catalogue

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId

/**
 * Internal function used to obtain the name of the Xodus store for the given [Name.EntityName].
 *
 * @return Store name.
 */
internal fun Name.EntityName.storeName(): String = "${DefaultCatalogue.ENTITY_STORE_PREFIX}_${this.schemaName}_${this.entityName}"

/**
 * Internal function used to obtain the name of the Xodus store for the given [Name.ColumnName].
 *
 * @return Store name.
 */
internal fun Name.ColumnName.storeName(): String = "${DefaultCatalogue.COLUMN_STORE_PREFIX}_${this.schemaName}_${this.entityName}_${this.columnName}"

/**
 * Internal function used to obtain the name of the Xodus store for the given [Name.IndexName].
 *
 * @return Store name.
 */
internal fun Name.IndexName.storeName(): String = "${DefaultCatalogue.INDEX_STORE_PREFIX}_${this.schemaName}_${this.entityName}_${this.indexName}"

/**
 * Converts [TupleId] to an [ArrayByteIterable] used for persistence through Xodus.
 */
fun TupleId.toKey(): ArrayByteIterable = LongBinding.longToCompressedEntry(this)