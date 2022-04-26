package org.vitrivr.cottontail.storage.lucene

/**
 * Configuration class by for [XodusDirectory].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class XodusDirectoryConfig(val inputBufferSize: Int = 1024, val inputMergeBufferSize: Int = 4096)