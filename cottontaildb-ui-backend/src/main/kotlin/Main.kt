import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.*
import io.javalin.http.Context
import io.javalin.http.Header
import io.javalin.http.staticfiles.Location
import io.javalin.openapi.CookieAuth
import io.javalin.openapi.plugin.OpenApiPlugin
import io.javalin.openapi.plugin.OpenApiPluginConfiguration
import io.javalin.openapi.plugin.SecurityComponentConfiguration
import io.javalin.openapi.plugin.swagger.SwaggerConfiguration
import io.javalin.openapi.plugin.swagger.SwaggerPlugin
import io.javalin.plugin.bundled.CorsContainer
import io.javalin.plugin.bundled.CorsPluginConfig
import org.eclipse.jetty.server.session.DefaultSessionCache
import org.eclipse.jetty.server.session.FileSessionDataStore
import org.eclipse.jetty.server.session.SessionHandler
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.ui.api.entity.EntityController
import org.vitrivr.cottontail.ui.api.list.ListController
import org.vitrivr.cottontail.ui.api.query.QueryController
import org.vitrivr.cottontail.ui.api.schema.SchemaController
import org.vitrivr.cottontail.ui.api.session.connect
import org.vitrivr.cottontail.ui.api.session.connections
import org.vitrivr.cottontail.ui.api.session.disconnect
import org.vitrivr.cottontail.ui.api.system.SystemController
import org.vitrivr.cottontail.ui.json.KotlinxJsonMapper
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
        config.plugins.enableCors { cors: CorsContainer -> cors.add { it: CorsPluginConfig -> it.anyHost() } }
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
        config.jsonMapper(KotlinxJsonMapper)

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
            ctx.method()
            ctx.header(Header.ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            ctx.header(Header.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, PATCH, PUT, DELETE, OPTIONS")
            ctx.header(Header.ACCESS_CONTROL_ALLOW_HEADERS, "Authorization, Content-Type")
            ctx.header(Header.CONTENT_TYPE, "application/json")
        }

        /** Path to API related functionality. */
        path("api") {
            /** All paths related to session and connection handling. */
            path("session") {
                post("connect") { connect(it) }
                post("disconnect") { disconnect(it) }
                post("connections") { connections(it) }
            }
        }


        path("api/query"){
            post(QueryController::query)
        }
        path("api/list") {
            get(ListController::getList)
        }
        path("schemas") {
            get(SchemaController::listAllSchemas)
            path("{name}") {
                post(SchemaController::createSchema)
                get(SchemaController::listEntities)
                delete(SchemaController::dropSchema)
                path("data") {
                    get(SchemaController::dumpSchema)
                }
            }
        }
        path("entities") {
            get(EntityController::listAllEntities)
        }
        path("entities/{name}") {
            get(EntityController::aboutEntity)
            post(EntityController::createEntity)
            delete(EntityController::dropEntity)
            path("truncate") {
                delete(EntityController::truncateEntity)
            }
            path("clear") {
                delete(EntityController::clearEntity)
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
        path("api/system") {
            path("transactions") {
                get(SystemController::listTransactions)
                path("{txId}") {
                    delete(SystemController::killTransaction)
                }
            }
            path("locks") {
                get(SystemController::listLocks)
            }
        }
    }.start(7070)
}