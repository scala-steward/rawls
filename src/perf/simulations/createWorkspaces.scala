package default

import scala.concurrent.duration._
import java.io._

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.jdbc.Predef._

class createWorkspaces extends Simulation {

	//Helpers to set up the run

	val lines = scala.io.Source.fromFile("../user-files/config.txt").getLines
	val accessToken = lines.next
	val numUsers = lines.next.toInt

	def fileGenerator(f: java.io.File)(op: java.io.PrintWriter => Unit) {
		val p = new java.io.PrintWriter(f)
		try { op(p) } finally { p.close() }
	}

	val r = scala.util.Random
	val runID = s"gatling_creations_${r.nextInt % 1000}"

	val runIds = for(i <- 1 to numUsers) yield { Map("workspacename" -> s"${runID}_${i}") }

	//The run itself

	val httpProtocol = http
		.baseURL("https://rawls.dsde-dev.broadinstitute.org")
		.inferHtmlResources()

	val headers = Map("Authorization" -> s"Bearer ${accessToken}",
		"Content-Type" -> "application/json")

	val scn = scenario(s"createWorkspaces_${numUsers}")
    .feed(csv(s"../user-files/access_tokens.csv"))
		.feed(runIds)
		.exec(http("create_request")
      .post("/api/workspaces")
      .headers(Map("Authorization" -> "Bearer ${accessToken}", "Content-Type" -> "application/json"))
      .body(StringBody("""{"namespace": "${project}", "name": "${workspacename}", "attributes": {}}""")))

	//NOTE: be sure to re-configure time if needed
	setUp(scn.inject(rampUsers(numUsers) over(60 seconds))).protocols(httpProtocol)
}