package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.index.basic.IndexConfig
import java.io.ByteArrayInputStream

/**
 * The [IndexConfig] for the [UQBTreeIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object UQBTreeIndexConfig: IndexConfig<UQBTreeIndex>, ComparableBinding() {
    override fun readObject(stream: ByteArrayInputStream): Comparable<UQBTreeIndexConfig> = UQBTreeIndexConfig
    override fun writeObject(output: LightOutputStream, `object`: Comparable<UQBTreeIndexConfig>) {/* No op. */
    }
    override fun toMap(): Map<String, String> = emptyMap()
}