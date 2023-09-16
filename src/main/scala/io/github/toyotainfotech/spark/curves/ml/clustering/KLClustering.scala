/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package io.github.toyotainfotech.spark.curves.ml.clustering

import io.github.toyotainfotech.spark.curves.sql.StructTypeExtension
import io.github.toyotainfotech.spark.curves.sql.catalyst.expressions.*
import io.github.toyotainfotech.spark.curves.sql.functions.*
import org.apache.spark.ml.param.*
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.collection.mutable

trait KLClusteringParams extends Params {
  val inputCurveCol: Param[String] =
    new Param[String](this, "inputCurveCol", "input curve column name typed as array of arrays or array of structs")
  def getInputCurveCol: String = $(inputCurveCol)
  setDefault(inputCurveCol, "curve")

  val inputCurveAxisFields =
    new StringArrayParam(this, "inputCurveAxisFields", "axis field names if curve column is typed as array of structs")
  def getInputCurveAxisFields: Array[String] = $(inputCurveAxisFields)
  def encodeInput(): Column = {
    if (isSet(inputCurveAxisFields)) {
      transform(
        col($(inputCurveCol)),
        element => array($(inputCurveAxisFields).toSeq.map(field => element(field))*)
      )
    } else {
      col($(inputCurveCol))
    }
  }

  val outputClusterIndexCol = new Param[String](this, "outputClusterIndexCol", "output column name for cluster index")
  def getOutputClusterIndexCol: String = $(outputClusterIndexCol)
  setDefault(outputClusterIndexCol, "cluster_index")

  val outputSimilarityCol = new Param[String](this, "outputSimilarityCol", "output column name for curve similarity")
  def getOutputSimilarityCol: String = $(outputSimilarityCol)
  setDefault(outputSimilarityCol, "similarity")

  val outputMatchingCurveCol =
    new Param[String](this, "outputMatchingCurveCol", "output column name for matching curve (optional)")
  def getOutputMatchingCurveCol: String = $(outputMatchingCurveCol)

  val checkpoint = new BooleanParam(this, "checkpoint", "")
  def getCheckpoint: Boolean = $(checkpoint)
  setDefault(checkpoint -> false)

  val iter: IntParam = new IntParam(this, "iter", "number of iterations (>= 1)", ParamValidators.gtEq(1))
  def getIter: Int = $(iter)

  val k = new IntParam(this, "k", "number of clusters (>= 2)", ParamValidators.gtEq(2))
  def getK: Int = $(k)

  val l = new IntParam(this, "l", "number of points in simplified curve (>= 2)", ParamValidators.gtEq(2))
  def getL: Int = $(l)

  private val curveSimplifyNames = Seq(
    ArraySampleIndex.prettyName,
    PolylineSampleEquidistant.prettyName
  )
  def curveSimplify: Column => Column =
    $(curveSimplifyFunc) match {
      case ArraySampleIndex.prettyName          => array_sample_index(_, $(l))
      case PolylineSampleEquidistant.prettyName => polyline_sample_equidistant(_, $(l))
      case _                                    => throw new IllegalArgumentException
    }
  val curveSimplifyFunc = new Param[String](
    this,
    "curveSimplifyFunc",
    "one of " + curveSimplifyNames.mkString("[\"", "\", \"", "\"]"),
    curveSimplifyNames.contains(_: String)
  )
  def getCurveSimplifyFunc: String = $(curveSimplifyFunc)
  setDefault(curveSimplifyFunc -> PolylineSampleEquidistant.prettyName)

  private val curveSimilarityNames = Seq(
    ContinuousDynamicTimeWarping.prettyName,
    ContinuousFrechetDistance.prettyName,
    ContinuousHausdorffDistance.prettyName
  )
  def curveSimilarity: (Column, Column) => Column =
    $(curveSimilarityFunc) match {
      case ContinuousDynamicTimeWarping.prettyName => continuous_dynamic_time_warping
      case ContinuousFrechetDistance.prettyName    => continuous_frechet_distance
      case ContinuousHausdorffDistance.prettyName  => continuous_hausdorff_distance
      case _                                       => throw new IllegalArgumentException
    }
  def curveSimilarityMatch: (Column, Column) => Column =
    $(curveSimilarityFunc) match {
      case ContinuousDynamicTimeWarping.prettyName => continuous_dynamic_time_warping_match
      case ContinuousFrechetDistance.prettyName    => continuous_frechet_distance_match
      case ContinuousHausdorffDistance.prettyName  => continuous_hausdorff_distance_match
      case _                                       => throw new IllegalArgumentException
    }
  val curveSimilarityFunc = new Param[String](
    this,
    "curveSimilarityFunc",
    "one of " + curveSimilarityNames.mkString("[\"", "\", \"", "\"]"),
    curveSimilarityNames.contains(_: String)
  )
  def getCurveSimilarityFunc: String = $(curveSimilarityFunc)
  setDefault(curveSimilarityFunc -> ContinuousDynamicTimeWarping.prettyName)

  private val pointsCenterNames = Seq(
    MeanCenter.prettyName,
    SmallestCircleCenter.prettyName,
    SmallestRectangleCenter.prettyName
  )
  def pointsCenter: Column => Column =
    $(pointsCenterFunc) match {
      case MeanCenter.prettyName              => mean_center
      case SmallestCircleCenter.prettyName    => smallest_circle_center
      case SmallestRectangleCenter.prettyName => smallest_rectangle_center
      case _                                  => throw new IllegalArgumentException
    }
  val pointsCenterFunc = new Param[String](
    this,
    "pointsCenterFunc",
    "one of " + pointsCenterNames.mkString("[\"", "\", \"", "\"]"),
    pointsCenterNames.contains(_: String)
  )
  def getPointsCenterFunc: String = $(pointsCenterFunc)
  setDefault(pointsCenterFunc -> MeanCenter.prettyName)
}

class KLClustering(override val uid: String) extends Estimator[KLClusteringModel] with KLClusteringParams {
  def this() = this(Identifiable.randomUID(getClass.getName))

  def setInputCurveCol(value: String): this.type = set(inputCurveCol, value)
  def setInputCurveAxisFields(value: Array[String]): this.type = set(inputCurveAxisFields, value)
  def setOutputClusterIndexCol(value: String): this.type = set(outputClusterIndexCol, value)
  def setOutputSimilarityCol(value: String): this.type = set(outputSimilarityCol, value)
  def setOutputMatchingCurveCol(value: String): this.type = set(outputMatchingCurveCol, value)
  def setCheckpoint(value: Boolean): this.type = set(checkpoint, value)
  def setIter(value: Int): this.type = set(iter, value)
  def setK(value: Int): this.type = set(k, value)
  def setL(value: Int): this.type = set(l, value)
  def setCurveSimilarityFunc(value: String): this.type = set(curveSimilarityFunc, value)
  def setCurveSimplifyFunc(value: String): this.type = set(curveSimplifyFunc, value)
  def setPointsCenterFunc(value: String): this.type = set(pointsCenterFunc, value)

  override def fit(dataset: Dataset[?]): KLClusteringModel = {
    val curves = dataset
      .select(encodeInput() as "curve")
      .withColumn("id", monotonically_increasing_id())
      .toDF()
      .cache()

    var clusteredCurves = initializeCluster(curves).cache()
    var clusterMetrics = measureClusterMetrics(clusteredCurves, 0).cache()

    // trigger action to cache
    clusterMetrics.isEmpty

    for (i <- 1 until $(iter)) {
      clusteredCurves = updateCluster(curves, updateCenter(clusteredCurves)).cache()
      clusterMetrics = clusterMetrics.union(measureClusterMetrics(clusteredCurves, i)).cache()

      // trigger action to cache
      clusterMetrics.isEmpty
    }
    var centers = updateCenter(clusteredCurves).cache()

    // trigger action to cache
    if ($(checkpoint)) {
      clusterMetrics = clusterMetrics.checkpoint()
      centers = centers.checkpoint()
    } else {
      clusterMetrics.isEmpty
      centers.isEmpty
    }

    val model = new KLClusteringModel(centers, clusterMetrics)
    model.setParent(this)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    val s = schema
      .withField(StructField($(outputClusterIndexCol), IntegerType))
      .withField(StructField($(outputSimilarityCol), DoubleType))
    if (isSet(outputMatchingCurveCol)) {
      s.withField(StructField($(outputMatchingCurveCol), ArrayType(ArrayType(DoubleType))))
    } else {
      s
    }
  }

  override def copy(extra: ParamMap): Estimator[KLClusteringModel] = defaultCopy(extra)

  private def initializeCluster(
      curves: DataFrame
  ): DataFrame = {
    val count = curves.count()
    assert(count >= 1)

    // trigger action to cache and reset lineage to avoid that the exponential sql plan growth
    val firstCenterCurve = {
      var result = Array[Row]()
      while (result.isEmpty) {
        result = curves
          .sample((5.0 / count).min(1.0)) // e^-5 < 1%
          .select(curveSimplify(col("curve")) as "value")
          .head(1)
      }
      result.head
    }.getAs[mutable.Seq[mutable.Seq[Double]]]("value")

    var clusteredTrajectories = curves
      .select(
        col("curve"),
        lit(0) as "cluster_index",
        typedLit(firstCenterCurve) as "center_curve",
        curveSimilarity(col("curve"), typedLit(firstCenterCurve)) as "similarity"
      )

    for (i <- 1 until $(k)) {
      clusteredTrajectories.cache()

      // trigger action to cache and reset lineage to avoid that the exponential sql plan growth
      val nextCenterCurve =
        clusteredTrajectories
          .select(curveSimplify(max_by(col("curve"), col("similarity"))) as "value")
          .head()
          .getAs[mutable.Seq[mutable.Seq[Double]]]("value")

      clusteredTrajectories = clusteredTrajectories
        .withColumn(
          "new_similarity",
          curveSimilarity(col("curve"), typedLit(nextCenterCurve))
        )
        .select(
          col("curve"),
          when(col("similarity") < col("new_similarity"), col("cluster_index"))
            .otherwise(lit(i)) as "cluster_index",
          when(col("similarity") < col("new_similarity"), col("center_curve"))
            .otherwise(typedLit(nextCenterCurve)) as "center_curve",
          when(col("similarity") < col("new_similarity"), col("similarity"))
            .otherwise(col("new_similarity")) as "similarity"
        )
    }
    clusteredTrajectories
  }

  private def updateCluster(curves: DataFrame, centers: DataFrame): DataFrame =
    curves
      .crossJoin(centers.hint("broadcast"))
      .withColumn(
        "similarity",
        curveSimilarity(col("curve"), col("center_curve"))
      )
      .groupBy("id")
      .agg(
        first(col("curve")) as "curve",
        min_by(col("cluster_index"), col("similarity")) as "cluster_index",
        min_by(col("center_curve"), col("similarity")) as "center_curve",
        min("similarity") as "similarity"
      )

  private def updateCenter(clusteredCurves: DataFrame): DataFrame =
    clusteredCurves
      .select(
        col("cluster_index"),
        posexplode(
          curveSimilarityMatch(col("center_curve"), col("curve"))
        ) as Seq("center_pos", "matching_point")
      )
      .groupBy("cluster_index", "center_pos")
      .agg(pointsCenter(collect_list("matching_point")) as "center_point")
      .groupBy(col("cluster_index"))
      .agg(
        transform(
          array_sort(collect_list(struct(col("center_pos"), col("center_point")))),
          _("center_point")
        ) as "center_curve"
      )

  private def measureClusterMetrics(clusteredCurves: DataFrame, i: Int): DataFrame =
    clusteredCurves
      .groupBy("cluster_index")
      .agg(
        first("center_curve") as "center_curve",
        count("*") as "num",
        min("similarity") as "similarity_min",
        mean("similarity") as "similarity_mean",
        max("similarity") as "similarity_max",
        stddev("similarity") as "similarity_stddev"
      )
      .select(
        lit(i) as "iter",
        col("cluster_index"),
        col("center_curve"),
        col("num"),
        col("similarity_min"),
        col("similarity_mean"),
        col("similarity_max"),
        col("similarity_stddev")
      )
}

class KLClusteringModel(override val uid: String, val centers: DataFrame, val clusterMetrics: DataFrame)
    extends Model[KLClusteringModel]
    with KLClusteringParams {
  def this(centers: DataFrame, clusterMetrics: DataFrame) =
    this(Identifiable.randomUID(getClass.getName), centers, clusterMetrics)

  override def transform(dataset: Dataset[?]): DataFrame = {
    val curves = dataset
      .withColumn("__id", monotonically_increasing_id())
      .cache()

    val clusteredCurves = curves
      .select(
        col("__id"),
        encodeInput() as "__curve"
      )
      .crossJoin(
        centers
          .select(col("cluster_index") as "__cluster_index", col("center_curve") as "__center_curve")
          .hint("broadcast")
      )
      .withColumn(
        $(outputSimilarityCol),
        curveSimilarity(col("__curve"), col("__center_curve"))
      )
      .groupBy("__id")
      .agg(
        min_by(col("__cluster_index"), col($(outputSimilarityCol))) as $(outputClusterIndexCol),
        (Seq(
          min($(outputSimilarityCol)) as $(outputSimilarityCol)
        ) ++ (if (isSet(outputMatchingCurveCol)) {
                Seq(
                  curveSimilarityMatch(
                    min_by(col("__center_curve"), col($(outputSimilarityCol))),
                    first(col("__curve"))
                  ) as $(outputMatchingCurveCol)
                )
              } else {
                Seq()
              }))*
      )
      .cache()

    // trigger action to cache
    clusteredCurves.isEmpty

    curves.join(clusteredCurves, "__id").drop("__id")
  }

  override def transformSchema(schema: StructType): StructType = {
    val s = schema
      .withField(StructField($(outputClusterIndexCol), IntegerType))
      .withField(StructField($(outputSimilarityCol), DoubleType))
    if (isSet(outputMatchingCurveCol)) {
      s.withField(StructField($(outputMatchingCurveCol), ArrayType(ArrayType(DoubleType))))
    } else {
      s
    }
  }

  override def copy(extra: ParamMap): KLClusteringModel = defaultCopy(extra)
}
