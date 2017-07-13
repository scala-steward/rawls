package org.broadinstitute.dsde.rawls.dataaccess.slick

/**
 * Created by thibault on 6/1/16.
 */
class DataAccessSpec extends TestDriverComponentWithFlatSpecAndMatchers {

  import driver.api._

  "DataAccess" should "test that truncateAll has left the DB in a known empty state" in withEmptyTestDatabase {
    val tables = runAndWait(safeTableNames)

    tables foreach { tableName =>
      val count = sql"SELECT COUNT(*) FROM #$tableName "
      assertResult(0, tableName + " not empty") {
        runAndWait(count.as[Int].head)
      }
    }
  }
}
