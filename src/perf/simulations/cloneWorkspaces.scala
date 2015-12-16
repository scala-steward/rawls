package default

import scala.concurrent.duration._
import java.io._

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.jdbc.Predef._

class cloneWorkspaces extends Simulation {

	//Helpers to set up the run

	val lines = scala.io.Source.fromFile("../user-files/config.txt").getLines
	val accessToken = lines.next
	val numUsers = lines.next.toInt

	//function to help us generate TSVs per-run
	def fileGenerator(f: java.io.File)(op: java.io.PrintWriter => Unit) {
	  val p = new java.io.PrintWriter(f)
	  try { op(p) } finally { p.close() }
	}

	val r = scala.util.Random
	val runID = s"gatling_clones_${r.nextInt}"
	val runIds = for(i <- 1 to numUsers) yield { Map("workspacename" -> s"${runID}_${i}") }

	//The gatling run

	val httpProtocol = http
		.baseURL("https://rawls.dsde-dev.broadinstitute.org")
		.inferHtmlResources()

	val scn = scenario(s"cloneWorkspaces_${numUsers}")
		.feed(csv(s"../user-files/access_tokens.csv"))
		.feed(runIds)
//		.exec(http("share_workspace")
//			.patch("/api/workspaces/broad-dsde-dev/Workshopish/acl") //our workshop model workspace
//			.headers(Map("Authorization" -> s"Bearer ${accessToken}", "Content-Type" -> "application/json"))
//			.body(StringBody("""[{"email": "${project}@firecloud.org", "accessLevel": "READER"}]""")))
		.exec(http("clone_request")
			.post("/api/workspaces/broad-dsde-dev/Workshopish/clone") //our workshop model workspace
			.headers(Map("Authorization" -> "Bearer ${accessToken}", "Content-Type" -> "application/json"))
			.body(StringBody("""{"namespace": "${project}", "name": "${workspacename}", "attributes": {}}""")))

	setUp(scn.inject(rampUsers(numUsers) over(60 seconds))).protocols(httpProtocol) //ramp up n users over 60 seconds
}