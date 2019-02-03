package ch.unibas.dmi.dbis.cottontail.server.grpc.services

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDDLGrpc

/**
 *
 */
internal class CottonDDLService (val catalogue: Catalogue): CottonDDLGrpc.CottonDDLImplBase() {

}