package ch.unibas.dmi.dbis.cottontail.server.grpc.services

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDQLGrpc

internal class CottonDQLService (val catalogue: Catalogue): CottonDQLGrpc.CottonDQLImplBase() {

}