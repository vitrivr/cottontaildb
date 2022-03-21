package org.vitrivr.cottontail.config

import jetbrains.exodus.env.EnvironmentConfig
import kotlinx.serialization.Serializable

@Serializable
data class XodusConfig (
    val logFileSize: Long = 128 * 1024L, /** Determines size of an individual Log file on disk. @see [EnvironmentConfig.LOG_FILE_SIZE] */
    val logCachePageSize: Int = 128 * 1024, /** Determines size of an individual Log page in cache. @see [EnvironmentConfig.LOG_CACHE_PAGE_SIZE] */
    val logCacheOpenFilesSize: Int = 500, /** Determines size of the open files cache. @see [EnvironmentConfig.LOG_CACHE_OPEN_FILES] */
    val logCacheReadAhead: Int = 25, /** Determines if and how many pages should be pre-fetched. @see [EnvironmentConfig.LOG_CACHE_READ_AHEAD_MULTIPLE] */
    val storeGetCacheSize: Int = 500, /** Determines size of internal store-get cache. @see [EnvironmentConfig.ENV_STOREGET_CACHE_SIZE] */
    val treeDupMaximumPageSize: Int = 128, /** Determines maximum size of a duplicate B+-tree page. @see [EnvironmentConfig.TREE_DUP_MAX_PAGE_SIZE] */
    val treeMaximumPageSize: Int = 1024 /** Determines the maximum size an individual B+-tree page. @see [EnvironmentConfig.TREE_MAX_PAGE_SIZE] */
) {
    /**
     * Converts this [XodusConfig] to an [EnvironmentConfig] usable by Xodus.
     *
     * @return [EnvironmentConfig].
     */
    fun toEnvironmentConfig(): EnvironmentConfig = EnvironmentConfig()
        .setLogCachePageSize(this.logCachePageSize)
        .setLogFileSize(this.logFileSize)
        .setLogCacheReadAheadMultiple(this.logCacheReadAhead)
        .setLogCacheOpenFilesCount(this.logCacheOpenFilesSize)
        .setEnvStoreGetCacheSize(this.storeGetCacheSize)
        .setTreeDupMaxPageSize(this.treeDupMaximumPageSize)
        .setTreeMaxPageSize(this.treeMaximumPageSize)
}