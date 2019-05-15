package ch.unibas.dmi.dbis.cottontail.cli

import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import com.google.protobuf.Empty

fun Entity(name: String, schema: CottontailGrpc.Schema): CottontailGrpc.Entity = CottontailGrpc.Entity.newBuilder().setName(name).setSchema(schema).build()

fun Schema(name: String) = CottontailGrpc.Schema.newBuilder().setName(name).build()

fun From(entity: CottontailGrpc.Entity): CottontailGrpc.From = CottontailGrpc.From.newBuilder().setEntity(entity).build()

fun Projection(operation: CottontailGrpc.Projection.Operation): CottontailGrpc.Projection = CottontailGrpc.Projection.newBuilder().setOp(operation).build()

fun Where(predicate: CottontailGrpc.AtomicLiteralBooleanPredicate): CottontailGrpc.Where = CottontailGrpc.Where.newBuilder().setAtomic(predicate).build()

fun MatchAll(): CottontailGrpc.Projection = CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.SELECT).putAttributes("*", "").build()

fun Empty(): Empty = Empty.getDefaultInstance()