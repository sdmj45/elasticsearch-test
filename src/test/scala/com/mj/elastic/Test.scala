package com.mj.elastic

import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

class Test extends AnyFeatureSpec with Matchers {

  Scenario("Test elasticsearch server") {
    val esClient = ElasticsearchTestServer.global.client
    println(esClient.cluster().stats().status().toString)
    assertResult("Green")(esClient.cluster().stats().status().toString)
  }
}
