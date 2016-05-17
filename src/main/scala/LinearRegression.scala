import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors

object LinearRegression {
    def main(args: Array[String]) {
        // Set context
        val conf = new SparkConf().setAppName("Linear Regression")
        val sc = new SparkContext(conf)
        // Load and parse the data
        val root = "///usr/local/Cellar/apache-spark/1.6.0/libexec/"
        val data = sc.textFile(root + "data/mllib/ridge-data/lpsa.data")
        val parsedData = data.map {line =>
            val parts = line.split(',')
            LabeledPoint(parts(0).toDouble, 
                Vectors.dense(parts(1).split(' ').map(_.toDouble)))
            }.cache()

        // Building the model
        val numIterations = 1000
        val stepSize = 0.001
        val model = LinearRegressionWithSGD.train(parsedData,
            numIterations, stepSize)

        // Evaluate model on training examples and compute training error
        val valuesAndPreds = parsedData.map {point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
        }

        val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
        println("Training Mean Squared Error = " + MSE)
    }
}
