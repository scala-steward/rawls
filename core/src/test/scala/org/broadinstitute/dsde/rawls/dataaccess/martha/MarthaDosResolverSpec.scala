package org.broadinstitute.dsde.rawls.dataaccess.martha

import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class MarthaDosResolverSpec extends FlatSpecLike with Matchers with BeforeAndAfterAll {

  "Martha DOS resolver" should "skip Jade dev" in {
    MarthaDosResolver.isExcludedDomain(
      "drs://jade.datarepo-dev.broadinstitute.org/v1_0c86170e-312d-4b39-a0a4-2a2bfaa24c7a_c0e40912-8b14-43f6-9a2f-b278144d0060",
      Seq("asdf", "jade.datarepo-dev.broadinstitute.org")
    ) shouldBe true
  }

  "Martha DOS resolver" should "not skip dg" in {
    MarthaDosResolver.isExcludedDomain(
      "drs://dg.712C/fa640b0e-9779-452f-99a6-16d833d15bd0",
      Seq("asdf", "jade.datarepo-dev.broadinstitute.org")
    ) shouldBe false
  }

  "Martha DOS resolver" should "not explode on URI parse fail" in {
    MarthaDosResolver.isExcludedDomain(
      "zardoz",
      Seq("asdf", "jade.datarepo-dev.broadinstitute.org")
    ) shouldBe false
  }

}
