package org.vitrivr.cottontail.legacy

import java.nio.file.Path

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Migrator {


    /**
     *
     */
    fun migrate(catalogue: Path, destination: Path)
}