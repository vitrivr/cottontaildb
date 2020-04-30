package org.vitrivr.cottontail

import ch.unibas.dmi.dbis.cottontail.config.Config
import java.nio.file.Paths


object TestConstants {


    val config = Config(Paths.get("./cotton-test"), true)
}
