/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import scala.language.existentials

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.{DataType, ObjectType}

/**
 * Takes the input row from child and turns it into object using the given deserializer expression.
 * The output of this operator is a single-field safe row containing the deserialized object.
 */
case class DeserializeToObject(
    deserializer: Expression,
    outputObjAttr: Attribute,
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = outputObjAttr :: Nil
  override def producedAttributes: AttributeSet = AttributeSet(outputObjAttr)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val bound = ExpressionCanonicalizer.execute(
      BindReferences.bindReference(deserializer, child.output))
    ctx.currentVars = input
    val resultVars = bound.genCode(ctx) :: Nil
    consume(ctx, resultVars)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val projection = GenerateSafeProjection.generate(deserializer :: Nil, child.output)
      iter.map(projection)
    }
  }
}

/**
 * Takes the input object from child and turns in into unsafe row using the given serializer
 * expression.  The output of its child must be a single-field row containing the input object.
 */
case class SerializeFromObjectExec(
    serializer: Seq[NamedExpression],
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = serializer.map(_.toAttribute)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val bound = serializer.map { expr =>
      ExpressionCanonicalizer.execute(BindReferences.bindReference(expr, child.output))
    }
    ctx.currentVars = input
    val resultVars = bound.map(_.genCode(ctx))
    consume(ctx, resultVars)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val projection = UnsafeProjection.create(serializer)
      iter.map(projection)
    }
  }
}

/**
 * Helper functions for physical operators that work with user defined objects.
 */
trait ObjectOperator extends SparkPlan {
  def deserializeRowToObject(
      deserializer: Expression,
      inputSchema: Seq[Attribute]): InternalRow => Any = {
    val proj = GenerateSafeProjection.generate(deserializer :: Nil, inputSchema)
    (i: InternalRow) => proj(i).get(0, deserializer.dataType)
  }

  def serializeObjectToRow(serializer: Seq[Expression]): Any => UnsafeRow = {
    val proj = GenerateUnsafeProjection.generate(serializer)
    val objType = serializer.head.collect { case b: BoundReference => b.dataType }.head
    val objRow = new SpecificMutableRow(objType :: Nil)
    (o: Any) => {
      objRow(0) = o
      proj(objRow)
    }
  }

  def wrapObjectToRow(objType: DataType): Any => InternalRow = {
    val outputRow = new SpecificMutableRow(objType :: Nil)
    (o: Any) => {
      outputRow(0) = o
      outputRow
    }
  }

  def unwrapObjectFromRow(objType: DataType): InternalRow => Any = {
    (i: InternalRow) => i.get(0, objType)
  }
}

/**
 * Applies the given function to input object iterator.
 * The output of its child must be a single-field row containing the input object.
 */
case class MapPartitionsExec(
    func: Iterator[Any] => Iterator[Any],
    outputObjAttr: Attribute,
    child: SparkPlan)
  extends UnaryExecNode with ObjectOperator {

  override def output: Seq[Attribute] = outputObjAttr :: Nil
  override def producedAttributes: AttributeSet = AttributeSet(outputObjAttr)

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getObject = unwrapObjectFromRow(child.output.head.dataType)
      val outputObject = wrapObjectToRow(outputObjAttr.dataType)
      func(iter.map(getObject)).map(outputObject)
    }
  }
}

/**
 * Applies the given function to each input object.
 * The output of its child must be a single-field row containing the input object.
 *
 * This operator is kind of a safe version of [[ProjectExec]], as its output is custom object,
 * we need to use safe row to contain it.
 */
case class MapElementsExec(
    func: AnyRef,
    outputObjAttr: Attribute,
    child: SparkPlan)
  extends UnaryExecNode with ObjectOperator with CodegenSupport {

  override def output: Seq[Attribute] = outputObjAttr :: Nil
  override def producedAttributes: AttributeSet = AttributeSet(outputObjAttr)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val (funcClass, methodName) = func match {
      case m: MapFunction[_, _] => classOf[MapFunction[_, _]] -> "call"
      case _ => classOf[Any => Any] -> "apply"
    }
    val funcObj = Literal.create(func, ObjectType(funcClass))
    val callFunc = Invoke(funcObj, methodName, outputObjAttr.dataType, child.output)

    val bound = ExpressionCanonicalizer.execute(
      BindReferences.bindReference(callFunc, child.output))
    ctx.currentVars = input
    val resultVars = bound.genCode(ctx) :: Nil

    consume(ctx, resultVars)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val callFunc: Any => Any = func match {
      case m: MapFunction[_, _] => i => m.asInstanceOf[MapFunction[Any, Any]].call(i)
      case _ => func.asInstanceOf[Any => Any]
    }

    child.execute().mapPartitionsInternal { iter =>
      val getObject = unwrapObjectFromRow(child.output.head.dataType)
      val outputObject = wrapObjectToRow(outputObjAttr.dataType)
      iter.map(row => outputObject(callFunc(getObject(row))))
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

/**
 * Applies the given function to each input row, appending the encoded result at the end of the row.
 */
case class AppendColumnsExec(
    func: Any => Any,
    deserializer: Expression,
    serializer: Seq[NamedExpression],
    child: SparkPlan) extends UnaryExecNode with ObjectOperator {

  override def output: Seq[Attribute] = child.output ++ serializer.map(_.toAttribute)

  private def newColumnSchema = serializer.map(_.toAttribute).toStructType

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getObject = deserializeRowToObject(deserializer, child.output)
      val combiner = GenerateUnsafeRowJoiner.create(child.schema, newColumnSchema)
      val outputObject = serializeObjectToRow(serializer)

      iter.map { row =>
        val newColumns = outputObject(func(getObject(row)))
        combiner.join(row.asInstanceOf[UnsafeRow], newColumns): InternalRow
      }
    }
  }
}

/**
 * An optimized version of [[AppendColumnsExec]], that can be executed
 * on deserialized object directly.
 */
case class AppendColumnsWithObjectExec(
    func: Any => Any,
    inputSerializer: Seq[NamedExpression],
    newColumnsSerializer: Seq[NamedExpression],
    child: SparkPlan) extends UnaryExecNode with ObjectOperator {

  override def output: Seq[Attribute] = (inputSerializer ++ newColumnsSerializer).map(_.toAttribute)

  private def inputSchema = inputSerializer.map(_.toAttribute).toStructType
  private def newColumnSchema = newColumnsSerializer.map(_.toAttribute).toStructType

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val getChildObject = unwrapObjectFromRow(child.output.head.dataType)
      val outputChildObject = serializeObjectToRow(inputSerializer)
      val outputNewColumnOjb = serializeObjectToRow(newColumnsSerializer)
      val combiner = GenerateUnsafeRowJoiner.create(inputSchema, newColumnSchema)

      iter.map { row =>
        val childObj = getChildObject(row)
        val newColumns = outputNewColumnOjb(func(childObj))
        combiner.join(outputChildObject(childObj), newColumns): InternalRow
      }
    }
  }
}

/**
 * Groups the input rows together and calls the function with each group and an iterator containing
 * all elements in the group.  The result of this function is flattened before being output.
 */
case class MapGroupsExec(
    func: (Any, Iterator[Any]) => TraversableOnce[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    outputObjAttr: Attribute,
    child: SparkPlan) extends UnaryExecNode with ObjectOperator {

  override def output: Seq[Attribute] = outputObjAttr :: Nil
  override def producedAttributes: AttributeSet = AttributeSet(outputObjAttr)

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val grouped = GroupedIterator(iter, groupingAttributes, child.output)

      val getKey = deserializeRowToObject(keyDeserializer, groupingAttributes)
      val getValue = deserializeRowToObject(valueDeserializer, dataAttributes)
      val outputObject = wrapObjectToRow(outputObjAttr.dataType)

      grouped.flatMap { case (key, rowIter) =>
        val result = func(
          getKey(key),
          rowIter.map(getValue))
        result.map(outputObject)
      }
    }
  }
}

/**
 * Co-groups the data from left and right children, and calls the function with each group and 2
 * iterators containing all elements in the group from left and right side.
 * The result of this function is flattened before being output.
 */
case class CoGroupExec(
    func: (Any, Iterator[Any], Iterator[Any]) => TraversableOnce[Any],
    keyDeserializer: Expression,
    leftDeserializer: Expression,
    rightDeserializer: Expression,
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    leftAttr: Seq[Attribute],
    rightAttr: Seq[Attribute],
    outputObjAttr: Attribute,
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode with ObjectOperator {

  override def output: Seq[Attribute] = outputObjAttr :: Nil
  override def producedAttributes: AttributeSet = AttributeSet(outputObjAttr)

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftGroup) :: ClusteredDistribution(rightGroup) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    leftGroup.map(SortOrder(_, Ascending)) :: rightGroup.map(SortOrder(_, Ascending)) :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    left.execute().zipPartitions(right.execute()) { (leftData, rightData) =>
      val leftGrouped = GroupedIterator(leftData, leftGroup, left.output)
      val rightGrouped = GroupedIterator(rightData, rightGroup, right.output)

      val getKey = deserializeRowToObject(keyDeserializer, leftGroup)
      val getLeft = deserializeRowToObject(leftDeserializer, leftAttr)
      val getRight = deserializeRowToObject(rightDeserializer, rightAttr)
      val outputObject = wrapObjectToRow(outputObjAttr.dataType)

      new CoGroupedIterator(leftGrouped, rightGrouped, leftGroup).flatMap {
        case (key, leftResult, rightResult) =>
          val result = func(
            getKey(key),
            leftResult.map(getLeft),
            rightResult.map(getRight))
          result.map(outputObject)
      }
    }
  }
}
