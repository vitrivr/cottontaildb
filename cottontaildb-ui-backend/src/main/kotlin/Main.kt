import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinFeature
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.*
import io.javalin.http.Context
import io.javalin.http.Header
import io.javalin.http.staticfiles.Location
import io.javalin.json.JavalinJackson
import io.javalin.openapi.CookieAuth
import io.javalin.openapi.plugin.OpenApiPlugin
import io.javalin.openapi.plugin.OpenApiPluginConfiguration
import io.javalin.openapi.plugin.SecurityComponentConfiguration
import io.javalin.openapi.plugin.swagger.SwaggerConfiguration
import io.javalin.openapi.plugin.swagger.SwaggerPlugin
import org.eclipse.jetty.server.session.DefaultSessionCache
import org.eclipse.jetty.server.session.FileSessionDataStore
import org.eclipse.jetty.server.session.SessionHandler
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.ui.api.ddl.*
import org.vitrivr.cottontail.ui.api.query.QueryController
import org.vitrivr.cottontail.ui.api.session.connect
import org.vitrivr.cottontail.ui.api.session.connections
import org.vitrivr.cottontail.ui.api.session.disconnect
import org.vitrivr.cottontail.ui.api.system.killTransaction
import org.vitrivr.cottontail.ui.api.system.listLocks
import org.vitrivr.cottontail.ui.api.system.listTransactions
import org.vitrivr.cottontail.ui.model.status.ErrorStatus
import org.vitrivr.cottontail.ui.model.status.ErrorStatusException
import java.io.File
import java.util.concurrent.TimeUnit

fun initClient(context: Context): SimpleClient {
    val port = context.queryParam("port")
    val address = context.queryParam("address")
    require(port!= null && address != null) { context.status(400) }
    val channel = channelCache.get(Pair(port.toInt(),address))
    return SimpleClient(channel)
}

var channelCache: LoadingCache<Pair<Int,String>, ManagedChannel> =  Caffeine.newBuilder()
    .maximumSize(100)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build { key: Pair<Int,String> ->  ManagedChannelBuilder.forAddress(key.second, key.first).enableFullStreamDecompression().usePlaintext().build() }

var queryCache: LoadingCache<QueryController.QueryKey, QueryController.QueryData> =  Caffeine.newBuilder()
    .maximumSize(100)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build { key: QueryController.QueryKey ->
        QueryController.executeQuery(key.queryRequest, key.port, key.address) }

var pagedCache: LoadingCache<QueryController.QueryPagesKey, List<QueryController.Page>> =  Caffeine.newBuilder()
    .maximumSize(10000)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build { key: QueryController.QueryPagesKey -> QueryController.computePages(key.sessionID, key.queryRequest, key.port, key.address, key.pageSize)}

var pageCache:  LoadingCache<QueryController.QueryPageKey, QueryController.Page> =  Caffeine.newBuilder()
    .maximumSize(1000000)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build { key: QueryController.QueryPageKey -> QueryController.getPage(key.sessionID, key.queryRequest, key.port, key.address, key.pageSize, key.page)}

fun fileSessionHandler() = SessionHandler().apply {
    sessionCache = DefaultSessionCache(this).apply {
        sessionDataStore = FileSessionDataStore().apply {
            val baseDir = File(System.getProperty("java.io.tmpdir"))
            this.storeDir = File(baseDir, "javalin-session-store").apply { mkdir() }
        }
    }
    httpOnly = true
}

/**
 *
 */
fun main(args: Array<String>) {
    Javalin.create { config ->
        config.staticFiles.add{
            it.directory = "html"
            it.location = Location.CLASSPATH
        }
        config.jetty.sessionHandler { fileSessionHandler() }
        config.spaRoot.addFile("/", "html/index.html")
        config.plugins.enableCors { cors ->
            cors.add {
                it.reflectClientOrigin = true // anyHost() has similar implications and might be used in production? I'm not sure how to cope with production and dev here simultaneously
                it.allowCredentials = true
            }
        }

        /* Configure serialization using Jackson + Kotlin module. */
        val mapper = ObjectMapper().registerModule(
            KotlinModule.Builder()
            .configure(KotlinFeature.NullToEmptyCollection, true)
            .configure(KotlinFeature.NullToEmptyMap, true)
            .configure(KotlinFeature.NullIsSameAsDefault, true)
            .configure(KotlinFeature.SingletonSupport, true)
            .configure(KotlinFeature.StrictNullChecks, true)
            .build())
        config.jsonMapper(JavalinJackson(mapper))

        /* Registers Open API plugin. */
        config.plugins.register(
            OpenApiPlugin(
                OpenApiPluginConfiguration()
                    .withDocumentationPath("/swagger-docs")
                    .withDefinitionConfiguration { _, u ->
                        u.withOpenApiInfo { t ->
                            t.title = "Thumper API"
                            t.version = "1.0.0."
                            t.description = "API for Thumper, the official Cottontail DB UI Version 1.0.0"
                        }
                        u.withSecurity(
                            SecurityComponentConfiguration().withSecurityScheme("CookieAuth", CookieAuth("SESSIONID"))
                        )
                    }
            )
        )

        /* Registers Swagger Plugin. */
        config.plugins.register(
            SwaggerPlugin(
                SwaggerConfiguration().apply {
                    this.version = "4.10.3"
                    this.documentationPath = "/swagger-docs"
                    this.uiPath = "/swagger-ui"
                }
            )
        )
    }.routes {
        before { ctx ->
            ctx.header(Header.CONTENT_TYPE, "application/json")
        }

        /** Path to API related functionality. */
        path("api") {
            /** All paths related to session and connection handling. */
            path("session") {
                post("connect") { connect(it) }
                post("disconnect") { disconnect(it) }
                get("connections") { connections(it) }
            }

            /** All paths related to a specific connection. */
            path("{connection}") {
                get("list") { listSchemas(it) }
                post("{schema}") { createSchema(it) }
                delete("{schema}") { dropSchema(it) }
                path("{schema}") {
                    get("list") { listEntities(it) }
                    get("{entity}") { aboutEntity(it) }
                    post("{entity}") { createEntity(it) }
                    delete("{entity}") { dropEntity(it) }
                    path("{entity}") {
                        delete("{truncate}") { truncateEntity(it) }
                    }
                }


                get("transactions") { listTransactions(it) }
                path("transactions") {
                    delete("{txId}") { killTransaction(it) }
                }
                get("locks") { listLocks(it) }
            }
        }


        path("api/query") {
            post(QueryController::query)
        }
        /*path("api/list") {
            get(ListController::getList)
        }
        path("entities/{name}") {
            path("truncate") {
                delete(EntityController::truncateEntity)
            }
            path("data") {
                get(EntityController::dumpEntity)
                delete(EntityController::deleteRow)
                post(EntityController::insertRow)
                patch(EntityController::update)
            }
        }
        path("indexes/{name}") {
            post(EntityController::createIndex)
            delete(EntityController::dropIndex)
        }
        */
    }.exception(ErrorStatusException::class.java) { e, ctx ->
        ctx.status(e.code).json(e.toStatus())
    }.exception(Exception::class.java) { e, ctx ->
        ctx.status(500).json(ErrorStatus(500, "Internal server error: ${e.localizedMessage}"))
    }.start(7070)
}