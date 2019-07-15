package m4.resources

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util
import com.mongodb.spark._
import org.apache.hadoop.fs._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import scala.collection.mutable
import scala.util.parsing.json._

object Aire_calidad {

  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("org.apache.spark")
    logger.setLevel(Level.ERROR)

    // Se inicializan las configuraciones necesarias junto con la conexión con la base de datos y colección de MongoDB
    val spark = SparkSession
      .builder()
      .appName("Processing_tw")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.users")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.users")
      .getOrCreate()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)



    // Leer la copia del ultimo csv con la medición, las dos fuentes de códigos-estaciones y códgios-magintudes.
    val aire = spark.read.option("header", "true").csv("/user/abalserio/Grupal4_Flume/datosSpark/")
    val codigo_estaciones = spark.read.option("header", "true").csv("./src/main/scala/m4/resources/codigo_estaciones.csv")
    val codigo_magnitud = spark.read.option("header", "true").csv("./src/main/scala/m4/resources/codigo_magnitud.csv")



    // Con Spark SQL se prepara el csv añadiendo la fecha en formato fecha y limpiando los valores de las medidas
    aire.createOrReplaceTempView("air")
    val air2 = spark.sql("""select provincia, MUNICIPIO, ESTACION, MAGNITUD, PUNTO_MUESTREO, ANO, MES, DIA,
                              from_unixtime(unix_timestamp(
                              concat(cast(ANO as STRING),
                              RIGHT(concat('0',cast(MES as STRING)),2),
                              RIGHT(concat('0',cast(DIA as STRING)),2)),
                              "yyyyMMdd")) AS _id,
                              case when H01 like ('V%') then substring(H01, 2, length(H01)) else null end H01_ ,
                              case when H02 like ('V%') then substring(H02, 2, length(H02)) else null end H02_ ,
                              case when H03 like ('V%') then substring(H03, 2, length(H03)) else null end H03_ ,
                              case when H04 like ('V%') then substring(H04, 2, length(H04)) else null end H04_ ,
                              case when H05 like ('V%') then substring(H05, 2, length(H05)) else null end H05_ ,
                              case when H06 like ('V%') then substring(H06, 2, length(H06)) else null end H06_ ,
                              case when H07 like ('V%') then substring(H07, 2, length(H07)) else null end H07_ ,
                              case when H08 like ('V%') then substring(H08, 2, length(H08)) else null end H08_ ,
                              case when H09 like ('V%') then substring(H09, 2, length(H09)) else null end H09_ ,
                              case when H10 like ('V%') then substring(H10, 2, length(H10)) else null end H10_ ,
                              case when H11 like ('V%') then substring(H11, 2, length(H11)) else null end H11_ ,
                              case when H12 like ('V%') then substring(H12, 2, length(H12)) else null end H12_ ,
                              case when H13 like ('V%') then substring(H13, 2, length(H13)) else null end H13_ ,
                              case when H14 like ('V%') then substring(H14, 2, length(H14)) else null end H14_ ,
                              case when H15 like ('V%') then substring(H15, 2, length(H15)) else null end H15_ ,
                              case when H16 like ('V%') then substring(H16, 2, length(H16)) else null end H16_ ,
                              case when H17 like ('V%') then substring(H17, 2, length(H17)) else null end H17_ ,
                              case when H18 like ('V%') then substring(H18, 2, length(H18)) else null end H18_ ,
                              case when H19 like ('V%') then substring(H19, 2, length(H19)) else null end H19_ ,
                              case when H20 like ('V%') then substring(H20, 2, length(H20)) else null end H20_ ,
                              case when H21 like ('V%') then substring(H21, 2, length(H21)) else null end H21_ ,
                              case when H22 like ('V%') then substring(H22, 2, length(H22)) else null end H22_ ,
                              case when H23 like ('V%') then substring(H23, 2, length(H23)) else null end H23_ ,
                              case when H24 like ('V%') then substring(H24, 2, length(H24)) else null end H24_
                            from air""")



    // Se enriquecen los datos con los nombres reales de las estaciones y magnitudes en vez de los códigos
    val aire_tuning = air2.withColumn("codigo_direccion", split(col("PUNTO_MUESTREO"), "_").getItem(0)).join(codigo_estaciones, col("codigo_direccion")===col("Codigo_Estacion")).drop("codigo_direccion")
    val aire_tuning2 = aire_tuning.join(codigo_magnitud, col("MAGNITUD")===col("MAGNITUD_CODIGO")).drop("MAGNITUD_CODIGO")



    // Se procede a generar la estructura de lo que será el JSON que luego se guardará en MongoDB
    val df1 = aire_tuning2.select(col("Nombre_Estacion"), col("_id").as("Fecha"),
      struct(
        col("PUNTO_MUESTREO"),
        col("MAGNITUD_NOMBRE"),
        struct(
          col("H01_"),
          col("H02_"),
          col("H03_"),
          col("H04_"),
          col("H05_"),
          col("H06_"),
          col("H07_"),
          col("H08_"),
          col("H09_"),
          col("H10_"),
          col("H11_"),
          col("H12_"),
          col("H13_"),
          col("H14_"),
          col("H15_"),
          col("H16_"),
          col("H17_"),
          col("H18_"),
          col("H19_"),
          col("H20_"),
          col("H21_"),
          col("H22_"),
          col("H23_"),
          col("H24_")
        ).as("values")
      ).as("info")
    ).groupBy(
      "Fecha", "Nombre_Estacion"
    ).agg(
      collect_list("info").alias("info")
    )
    val df2 = df1.select(
      col("Fecha").alias("_id"), struct(col("Nombre_Estacion"), col("info")).as("details")).groupBy("_id").agg(collect_list("details").alias("details"))



    // Una vez generado el JSON se escribe en MongoDB, en la base de datos y colección que se ha configurado antes con el Sparksession
    MongoSpark.save(df2)


    // Se borra la carpeta donde se ha dejado una copia del csv una vez procesado
    fs.delete(new Path("/user/abalserio/Grupal4_Flume/datosSpark/"), true)




  }
}
