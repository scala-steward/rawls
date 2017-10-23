package org.broadinstitute.dsde.rawls.jobexec

import java.util.UUID

import org.broadinstitute.dsde.rawls.dataaccess.SlickWorkspaceContext
import org.broadinstitute.dsde.rawls.dataaccess.slick.{DataAccess, ReadWriteAction, TestData, TestDriverComponent}
import org.broadinstitute.dsde.rawls.jobexec.EntityAttrGen.EntityAttribute
import org.broadinstitute.dsde.rawls.jobexec.WdlGen.{ArrayWdl, OptionalWdl, PrimitiveWdl, Wdl}
import org.broadinstitute.dsde.rawls.model.{AttributeEntityReferenceList, AttributeName, AttributeNumber, AttributeString, AttributeValueRawJson, Entity, MethodConfiguration, MethodRepoMethod, SubmissionValidationValue, Workspace}
import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalacheck.Prop._
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.Map
import spray.json._

import scala.concurrent.ExecutionContext


/**
  * Created by rtitle on 10/22/17.
  */
class MethodConfigResolverPropertySpec extends FlatSpec with Checkers with TestDriverComponent {
  import driver.api._

  "mc parsing" should "eval correctly" in {
    check {

      forAll(genConfigData[Int]) { configData: ConfigData[Int] =>
        withCustomTestDatabase(configData) { _ =>
          val context = SlickWorkspaceContext(workspace)
          val resolvedInputs: Map[String, Seq[SubmissionValidationValue]] = runAndWait(testResolveInputs(context, configData.methodConfig, configData.entity, configData.wdl, this))
          val methodProps = resolvedInputs(configData.entity.name).map { svv: SubmissionValidationValue =>
            svv.inputName -> svv.value.get
          }
          val wdlInputs: String = MethodConfigResolver.propertiesToWdlInputs(methodProps.toMap)
          val expected = s"""{"${configData.wdlName}":${
            if (configData.entity.entityType == "SampleSet") configData.supportingEntities.map(_ => configData.matchingEntityAttr.toAttrString).mkString(",")
            else configData.matchingEntityAttr.toAttrString
          }}"""

          println("actual: " + wdlInputs)
          println("expected: " + expected)

          wdlInputs == expected
        }
      }
    }
  }

  //Test harness to call resolveInputsForEntities without having to go via the WorkspaceService
  def testResolveInputs(workspaceContext: SlickWorkspaceContext, methodConfig: MethodConfiguration, entity: Entity, wdl: String, dataAccess: DataAccess)
                       (implicit executionContext: ExecutionContext): ReadWriteAction[Map[String, Seq[SubmissionValidationValue]]] = {
    dataAccess.entityQuery.findEntityByName(workspaceContext.workspaceId, entity.entityType, entity.name).result flatMap { entityRecs =>
      MethodConfigResolver.gatherInputs(methodConfig, wdl) match {
        case scala.util.Failure(exception) =>
          DBIO.failed(exception)
        case scala.util.Success(methodInputs) =>
          MethodConfigResolver.evaluateInputExpressions(workspaceContext, methodInputs, entityRecs, dataAccess)
      }
    }
  }

  def genConfigData[A](implicit arb: Arbitrary[A]) = MCGen.mcGen[A].map { case (wdl, wdlName, matchingEntityAttr, entity, supportingEntities, methodConfig) =>
    ConfigData(wdl, wdlName, matchingEntityAttr, entity, supportingEntities, methodConfig)
  }

  val workspace = Workspace("workspaces", "test_workspace", Set.empty, UUID.randomUUID().toString(), "aBucket", currentTime(), currentTime(), "testUser", Map.empty, Map.empty, Map.empty)
  case class ConfigData[A](wdl: String,
                           wdlName: String,
                           matchingEntityAttr: EntityAttribute[A],
                           entity: Entity,
                           supportingEntities: List[Entity],
                           methodConfig: MethodConfiguration) extends TestData {
    override def save() = {
      DBIO.seq(
        workspaceQuery.save(workspace),
        withWorkspaceContext(workspace) { context =>
          val saveSupportEntities = DBIO.sequence(supportingEntities.map(e => entityQuery.save(context, e)))
          DBIO.seq(
            saveSupportEntities,
            entityQuery.save(context, entity),
            methodConfigurationQuery.create(context, methodConfig)
          )
        }
      )
    }
  }

}

object MCGen {
  val dummyMethod = MethodRepoMethod("method_namespace", "test_method", 1)

  def mcGen[A](implicit arb: Arbitrary[A]) = for {
    namespace <- genDbString
    name <- genDbString
    entityType <- Gen.oneOf("Sample", "SampleSet")
    wdlName <- genDbString
    entityAttrName <- genDbString
    wdl <- WdlGen.optWdls
    attributes <- Gen.listOf(EntityAttrGen.attributes[Int])
    matchingAttribute <- EntityAttrGen.attributes[A](wdl.nestingLevel)
    entities <- if (entityType == "Sample") EntityGen.sampleGen(entityAttrName, matchingAttribute, attributes).map(List(_))
                else EntityGen.sampleSetGen(entityAttrName, matchingAttribute, attributes)
  } yield {
    val qualifiedWdlName = s"w1.t1.$wdlName"
    val mc = MethodConfiguration(namespace, name, entityType, Map.empty, Map(qualifiedWdlName -> (
      if (entityType == "Sample") AttributeString(s"this.$entityAttrName") else AttributeString(s"this.samples.$entityAttrName"))
    ), Map.empty, dummyMethod)
    val entity :: remainingEntities = entities

    (wdl.toWdlString(wdlName), qualifiedWdlName, matchingAttribute, entity, remainingEntities, mc)
  }


  def genDbString: Gen[String] = for {
    cs <- Gen.nonEmptyListOf(Gen.alphaUpperChar).suchThat(_.size < 64)
  } yield cs.mkString


}

object EntityGen {
  def sampleGen[A](argName: String, matchingAttribute: EntityAttribute[A], otherAttributes: List[EntityAttribute[Any]]): Gen[Entity] = for {
    name <- MCGen.genDbString
    otherAttrNames <- Gen.listOfN(otherAttributes.size, MCGen.genDbString)
  } yield {
    Entity(name, "Sample",
      ((argName -> matchingAttribute) :: otherAttrNames.zip(otherAttributes)).map { case (k, v) =>
        AttributeName.withDefaultNS(k) -> AttributeValueRawJson(v.toAttrString.parseJson)
      }.toMap)
  }

  def sampleSetGen[A](argName: String, matchingAttribute: EntityAttribute[A], otherAttributes: List[EntityAttribute[Any]]): Gen[List[Entity]] = for {
    name <- MCGen.genDbString
    samples <- Gen.nonEmptyListOf(sampleGen(argName, matchingAttribute, otherAttributes))
  } yield {
    (Entity(name, "SampleSet",
      Map(AttributeName.withDefaultNS("samples") -> AttributeEntityReferenceList(
        samples.map(_.toReference)))) :: samples)
  }
}

object EntityAttrGen {
  def primitives[A](implicit arb: Arbitrary[A]) = for {
    a <- arb.arbitrary
  } yield PrimitiveAttribute(a)

  def arrays[A](implicit arb: Arbitrary[A]) = for {
    as <- listOfAttributes[A]
  } yield ArrayAttribute(as)

  def listOfAttributes[A](implicit arb: Arbitrary[A]): Gen[List[EntityAttribute[A]]] = {
    Gen.lzy(Gen.oneOf(Gen.listOfN(3, primitives[A]), Gen.listOfN(3, arrays[A])))
  }

  def attributes[A](implicit arb: Arbitrary[A]): Gen[EntityAttribute[A]] = {
    Gen.lzy(Gen.oneOf(primitives[A], arrays[A]))
  }

  def attributes[A](nestingLevel: Int)(implicit arb: Arbitrary[A]): Gen[EntityAttribute[A]] = {
    def go(depth: Int): Gen[List[EntityAttribute[A]]] = {
      Gen.lzy(Gen.listOfN(3,
        if (depth == nestingLevel - 1) primitives[A]
        else for {
          as <- go(depth + 1)
        } yield ArrayAttribute(as)
      ))
    }
    if (nestingLevel == 0) primitives[A]
    else go(0).map(ArrayAttribute(_))
  }

  sealed trait EntityAttribute[+A] {
    def toAttrString: String = {
      this match {
        case PrimitiveAttribute(a) => a.toString
        case ArrayAttribute(inner) => s"[${inner.map(_.toAttrString).mkString(",")}]"
      }
    }

    def nestingLevel: Int = this match {
      case PrimitiveAttribute(_) => 0
      case ArrayAttribute(inner) => 1 + inner.head.nestingLevel
    }
  }
  case class PrimitiveAttribute[A](a: A) extends EntityAttribute[A]
  case class ArrayAttribute[A](inner: List[EntityAttribute[A]]) extends EntityAttribute[A]
}

object WdlGen {
  def primitives = Gen.const(PrimitiveWdl("Int")) // Gen.oneOf("Int", "String").map(PrimitiveWdl)

  def arrays = for {
    w <- wdls
  } yield ArrayWdl(w)

  def wdls: Gen[Wdl] = {
    Gen.lzy(Gen.oneOf(primitives, arrays))
  }

  def optWdls: Gen[Wdl] = {
    Gen.lzy(Gen.oneOf(wdls, wdls.map(OptionalWdl)))
  }

  sealed trait Wdl {
    def typeStr: String

    def toWdlString(name: String): String = {
      s"""
         |task t1 {
         |  ${typeStr} $name
         |  command {
         |    echo $name
         |  }
         |}
         |
         |workflow w1 {
         |  call t1
         |}
       """.stripMargin
    }
    def nestingLevel: Int = this match {
      case PrimitiveWdl(_) => 0
      case OptionalWdl(inner) => inner.nestingLevel
      case ArrayWdl(inner) => 1 + inner.nestingLevel
    }
  }
  case class PrimitiveWdl(typeStr: String) extends Wdl
  case class OptionalWdl(inner: Wdl) extends Wdl {
    def typeStr = inner.typeStr + "?"
  }
  case class ArrayWdl[A](inner: Wdl) extends Wdl {
    def typeStr = s"Array[${inner.typeStr}]"
  }
}