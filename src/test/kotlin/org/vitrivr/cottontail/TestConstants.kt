package org.vitrivr.cottontail

import org.vitrivr.cottontail.config.Config
import java.nio.file.Paths


object TestConstants {
    val config = Config(root = Paths.get("./cotton-test"), cli=false)
}
