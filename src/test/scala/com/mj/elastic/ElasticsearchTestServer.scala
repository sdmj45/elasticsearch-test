package com.mj.elastic

import co.elastic.clients.elasticsearch._types.ErrorResponse
import co.elastic.clients.elasticsearch.{ElasticsearchAsyncClient, ElasticsearchClient}
import co.elastic.clients.json.{JsonData, JsonpDeserializer, JsonpMapper}
import co.elastic.clients.json.jsonb.JsonbJsonpMapper
import co.elastic.clients.transport.{ElasticsearchTransport, JsonEndpoint, Version}
import co.elastic.clients.transport.endpoints.DelegatingJsonEndpoint
import co.elastic.clients.transport.rest_client.RestClientTransport
import ElasticsearchTestServer.testServer
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClient
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.images.builder.dockerfile.DockerfileBuilder
import org.testcontainers.utility.DockerImageName

import java.io.IOException
import java.time.Duration
import scala.collection.JavaConverters._

class ElasticsearchTestServer(plugins: String*) extends AutoCloseable {
  private var esContainer: ElasticsearchContainer = _
  final private val jsonpMapper = new JsonbJsonpMapper()
  private var esRestClient: RestClient = _
  private var esTransport: RestClientTransport = _
  private var esClient: ElasticsearchClient = _
  private val esUsername: String = "elastic"
  private val esPassword: String = "changeme"

  def start(): ElasticsearchTestServer = {
    val version =
      if (Version.VERSION.major < 8)
        new Version(7, 17, 5, false)
      else
        new Version(8, 3, 3, false)
    val esImage = "docker.elastic.co/elasticsearch/elasticsearch:" + version

    val image: DockerImageName =
      if (plugins.isEmpty) {
        DockerImageName.parse(esImage)
      } else {
        val esWithPluginsImage =
          new ImageFromDockerfile()
            .withDockerfileFromBuilder((d: DockerfileBuilder) => {
              d.from(esImage)
              for (plugin <- plugins) {
                d.run("/usr/share/elasticsearch/bin/elasticsearch-plugin", "install", plugin)
              }
            }).get
        DockerImageName.parse(esWithPluginsImage).asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch")
      }

    esContainer = new ElasticsearchContainer(image)
      .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
      .withEnv("path.repo", "/tmp")
      .withStartupTimeout(Duration.ofSeconds(180))
      .withPassword(esPassword)

    esContainer.setPortBindings(List("9200:9200").asJava)
    esContainer.start()

    val port = esContainer.getMappedPort(9200)
    println(s"mymyport: $port")
    val useTLS = version.major >= 8
    val host = new HttpHost("localhost", port,
      if (useTLS)
        "https"
      else
        "http"
    )
    val sslContext =
      if (useTLS)
        esContainer.createSslContextFromCa
      else
        null
    val credsProv = new BasicCredentialsProvider
    credsProv.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esUsername, esPassword))

    esRestClient = RestClient
      .builder(host)
      .setHttpClientConfigCallback(
        (hc: HttpAsyncClientBuilder) =>
          hc.setDefaultCredentialsProvider(credsProv)
            .setSSLContext(sslContext)
      )
      .build
    esTransport = new RestClientTransport(esRestClient, jsonpMapper)
    esClient = new ElasticsearchClient(esTransport)
    this
  }

  override def close(): Unit = {
    if (this.equals(testServer)) {
      return
    }
    if (esContainer != null)
      esContainer.stop()
    esContainer = null
  }

  def container: ElasticsearchContainer = this.esContainer

  def restClient: RestClient = esRestClient

  def transport: ElasticsearchTransport = esTransport

  def mapper: JsonpMapper = jsonpMapper

  def client: ElasticsearchClient = esClient

  def asyncClient = new ElasticsearchAsyncClient(esClient._transport(), esClient._transportOptions)
}

object ElasticsearchTestServer {

  private var testServer: ElasticsearchTestServer = _

  def global: ElasticsearchTestServer = {
    if (testServer == null) {
      println("Starting global ES test server.")
      testServer = new ElasticsearchTestServer()
      testServer.start()
      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        println("Stopping global ES test server.")
        testServer.close()
      }))
    }
    testServer
  }

  @throws[IOException]
  def getJsonResponse[Req](client: ElasticsearchClient, request: Req): JsonData = {
    var endpoint: JsonEndpoint[Req, JsonData, ErrorResponse] = null
    try {
      endpoint = request.getClass.getDeclaredField("_ENDPOINT").get(null).asInstanceOf[JsonEndpoint[Req, JsonData, ErrorResponse]]
    } catch {
      case e@(_: IllegalAccessException | _: NoSuchFieldException) =>
        throw new RuntimeException(e)
    }
    val jsonEndpoint = new DelegatingJsonEndpoint[Req, JsonData, ErrorResponse](endpoint) {
      override def responseDeserializer: JsonpDeserializer[JsonData] = JsonData._DESERIALIZER
    }
    client._transport.performRequest(request, jsonEndpoint, client._transportOptions)
  }
}
