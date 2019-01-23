package udfs

object Udfs {
  def leverage(xi: Long, countPts: Long, meanX: Double, normXsq: Double): Double = {
    val h: Double = if (normXsq > 0.0) {
      (1 / countPts.toDouble) + (math.pow(xi.toDouble - meanX, 2) / normXsq)
    } else {
      0.0
    }
    h
  }
  
  def residual(xi: Long, yi: Long, regressionSlope: Double, regressionIntercept: Double): Double = {
    val pyi: Double = regressionIntercept + (regressionSlope * xi)
    yi.toDouble - pyi
  }
  
  val cooksDistance = (xi: Long, yi: Long, countPts: Long, meanX: Double, regressionSlope: Double, regressionIntercept: Double, mse: Double, normXsq: Double) => {
    val r = this.residual(xi, yi, regressionSlope, regressionIntercept)
    val h = this.leverage(xi, countPts, meanX, normXsq)
    val denom: Double = math.pow(1 - h, 2)
    val hfactor: Double = if (denom > 0.0) {
      h / denom
    } else {
      0.0
    }
    val denom2: Double = (2 * mse) * hfactor
    val d: Double = if (denom2 > 0) {
      math.pow(r, 2) / denom2
    } else {
      0.0
    }
    d
  }
  
  val isOutlier = (cookDistance: Double, cookDistanceMean: Double) => {
    val isOutlier = if (cookDistance < (7.0 * cookDistanceMean)) {
      0
    } else {
      1
    }
    isOutlier
  }
  
  val zScore = (yi: Long, meanY: Double, stddevY: Double) => {
    val zidx: Double = if (stddevY != 0.0) {
      math.abs((yi.toDouble - meanY) / stddevY)
    } else {
      0.0
    }
    zidx
  }
  
  val absDeviation = (yi: Long, medianY: Double) => {
    math.abs(yi.toDouble - medianY)
  }
  
  val modZScore = (yi: Long, medianY: Double, medianAbsDeviationY: Double) => {
    val modZScore = if (medianAbsDeviationY != 0.0) {
      0.6745 * (yi.toDouble - medianY) / medianAbsDeviationY
    } else {
      0.0
    }
    modZScore
  }
  
  val isModZScoreOutlier = (modZScore: Double) => {
    math.abs(modZScore) < 3.5
  }
}