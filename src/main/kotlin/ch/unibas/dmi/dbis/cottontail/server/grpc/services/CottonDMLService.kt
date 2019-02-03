package ch.unibas.dmi.dbis.cottontail.server.grpc.services

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDMLGrpc

internal class CottonDMLService (val catalogue: Catalogue): CottonDMLGrpc.CottonDMLImplBase() {

}