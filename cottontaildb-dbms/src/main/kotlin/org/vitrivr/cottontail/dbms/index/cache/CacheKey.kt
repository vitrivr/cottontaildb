package org.vitrivr.cottontail.dbms.index.cache

import org.vitrivr.cottontail.core.database.Name

/**
 * A [CacheKey] used in the [InMemoryIndexCache].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class CacheKey(val name: Name.IndexName, val range: LongRange)