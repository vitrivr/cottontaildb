import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import entity.EntityController
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.*
import io.javalin.http.Context
import io.javalin.http.Header
import io.javalin.http.staticfiles.Location
import io.javalin.plugin.bundled.CorsContainer
import io.javalin.plugin.bundled.CorsPluginConfig
import list.ListController
import org.eclipse.jetty.server.session.DefaultSessionCache
import org.eclipse.jetty.server.session.FileSessionDataStore
import org.eclipse.jetty.server.session.SessionHandler
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dml.Insert
import query.QueryController
import schema.SchemaController
import system.SystemController
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

fun main() {

    ClientConfig(1865).client.insert(Insert().into("warren.cineast.features_providedocr").value("id", "2").value("feature", "dsad"))

    val app = Javalin.create { config ->
        config.staticFiles.add{
            it.directory = "html"
            it.location = Location.CLASSPATH
        }
        config.jetty.sessionHandler { fileSessionHandler() }
        config.spaRoot.addFile("/", "html/index.html")
        config.plugins.enableCors { cors: CorsContainer -> cors.add { it: CorsPluginConfig -> it.anyHost() } }
    }.start(7070)

    app.routes {
        before { ctx ->
            ctx.method()
            ctx.header(Header.ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            ctx.header(Header.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, PATCH, PUT, DELETE, OPTIONS")
            ctx.header(Header.ACCESS_CONTROL_ALLOW_HEADERS, "Authorization, Content-Type")
            ctx.header(Header.CONTENT_TYPE, "application/json")
        }
        path("query"){
            post(QueryController::query)
        }
        path("list") {
            get(ListController::getList)
        }
        path("schemas") {
            get(SchemaController::listAllSchemas)
            post(SchemaController::createSchema)
            path("{name}") {
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
        path("system") {
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
    }
}