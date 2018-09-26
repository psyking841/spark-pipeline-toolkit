import java.util.Calendar
import com.pan.spark.batch.app.BatchAppBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._

object aaa extends BatchAppBase {

  val xxx = defaultSettings.getDefaultConfigs.getString("environment")
  val envConfig = defaultSettings.getDefaultConfigs
  //defaultSettings.getDefaultConfigs.entrySet().forEach(print(_))
//  for (x <- defaultSettings.getDefaultConfigs.getConfig(xxx).entrySet()) {
//    print(x)
//  }

  for(x <- envConfig.getConfig("inputs").root().entrySet()){
    //print("-Dinputs." + x.getKey + "=" + x.getValue + " ")
    println(x.getKey)
    getDataSourceFor(x.getKey)
  }

  print("a")
}