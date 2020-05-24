package org.vitrivr.cottontail.database.queries.planning.cost

object Costs {
    /* Cost read access to disk. TODO: Calculate based on local hardware. */
    const val DISK_ACCESS_READ = 1e-5f

    /* Cost read access to memory. */
    const val MEMORY_ACCESS_READ = 1e-6f

    /* Default selectivity for boolean predicates. */
    const val DEFAULT_SELECTIVITY = 0.5f
}