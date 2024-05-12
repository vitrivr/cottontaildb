package org.vitrivr.cottontail.dbms.catalogue

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.database.TupleId


/**
 * Converts [TupleId] to an [ArrayByteIterable] used for persistence through Xodus.
 */
fun TupleId.toKey(): ArrayByteIterable = LongBinding.longToCompressedEntry(this)