import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import entity.EntityController
import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.*
import io.javalin.http.Header
import io.javalin.http.staticfiles.Location
import io.javalin.plugin.bundled.CorsContainer
import io.javalin.plugin.bundled.CorsPluginConfig
import list.ListController
import org.eclipse.jetty.server.session.DefaultSessionCache
import org.eclipse.jetty.server.session.FileSessionDataStore
import org.eclipse.jetty.server.session.SessionHandler
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.iterators.TupleIterator
import query.QueryController
import schema.SchemaController
import system.SystemController
import java.io.File
import java.util.concurrent.TimeUnit


// TODO:  
/*var cache: LoadingCache<Pair<SimpleClient, QueryController.Query>, TupleIterator> =  Caffeine.newBuilder()
    .maximumSize(10000)
    .expireAfterWrite(10, TimeUnit.MINUTES)
    .build { key: Pair<SimpleClient, QueryController.Query> -> QueryController.executeQuery(key.first, key.second) };*/

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

        path("{port}") {
            path("query"){
                get(QueryController::query)
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
}