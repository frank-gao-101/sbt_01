package com.amway.dms

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}
import com.amway.dms.{Constant => C, Utils => U}

/*
   step 1: add column month and/or quarter
   step 2: filter based on the report type to be generated.
   step 3: aggregation
 */
class RptDriver(data_in: String, data_out: String,
                mode: Option[String],
                freq: Option[String],
                range_seq: Seq[(String, String)],
                hasRange: Boolean,
                prefix: Option[String]) {

  val prefix_s = prefix.get

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def init() = {
    val spark = SparkSession.builder.appName("dms-reports")
            .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val startTime = Utils.getCurrDateTime()
    logger.info(s"=== dms report processing starts at $startTime")

    val ora_df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(data_in)

    if (hasRange)
      processRange(ora_df, range_seq)
    else {
      val freq_s = freq.get
      val mode_s = mode.get
      if (mode_s == C.ALL)
        process_nonRange_mode_all(ora_df, mode_s, freq_s)
      else
        processNonRange(ora_df, mode_s, freq_s)
    }

    spark.stop()
    val endTime = Utils.getCurrDateTime()
    val timeDiff = Utils.getTimeDiff(startTime, endTime)
    logger.info(s"=== dms report processing ends at ${endTime}")
    logger.info(s"=== Duration: ${timeDiff}.")
  }

  def processRange(df: DataFrame, range: U.PairSeq) {
    val dfm = proc_step1(df, "Range")
    range.foreach(process_Range_Each(dfm, _))
  }

  def process_Range_Each(df: DataFrame, ele: (String, String)) {
    val delim = ele._1.slice(4,5)
    val morq = if (ele._1 == ele._2) ele._1 else ele._1 + C.RANGE_DOTS + ele._2
    val freq = if (delim == C.M) C.MM else C.QQ
    val query_filter = (ele._1 == ele._2) match {
      case true => freq + " == '" + ele._1 + "'"
      case false => freq + " >= '" + ele._1 + "' AND " + freq + " <= '" + ele._2 + "'"
    }

    val dfmb = proc_step3(proc_step2(df, query_filter, freq))
    PreSink(dfmb, morq, "Range", freq)
  }

  def process_nonRange_mode_all(df: DataFrame, mode: String, freq: String) {
    val dfm = proc_step1(df, freq)
    val freq_list = dfm.select(freq).distinct().rdd.map(r => r(0).toString).collect()
    freq_list.foreach(filterByEachFreq(dfm, mode, freq, _))
  }

  def filterByEachFreq(df: DataFrame, mode:String, freq: String, each_freq:String) {
    val query_filter = freq + " == '" + each_freq + "'"
    val df_each_freq = proc_step3(df.filter(query_filter).drop(freq))

    PreSink(df_each_freq, each_freq, mode, freq)
  }

  def processNonRange(df: DataFrame, mode: String, freq: String){
    logger.info(s"=== Processing ${freq}ly reports ...")

    val df_step1 = proc_step1(df, freq)
    val (last_m, last_q) = Utils.mq
    val morq = if (freq == C.MM) last_m else last_q
    val query_filter = mode match {
      case C.LAST =>
        freq + " == '" + morq + "'"
      case C.UPTO | C.ALL =>
        freq + " <= '" + morq + "'"
    }
    val df_step2 = proc_step2(df_step1, query_filter, freq)

    val dfmb = proc_step3(df_step2)
    PreSink(dfmb, morq, mode, freq)
  }

  def proc_step1(df: DataFrame, freq: String) = {

    val df1 = df.withColumn("TRX_DT", regexp_replace(col("TRX_DT"), "(\\s.+$)", ""))
    val dfm = if (hasRange) {
      df1.addColumnQuarter.addColumnMonth
    } else freq match {
      case C.MM => df1.addColumnMonth
      case C.QQ => df1.addColumnQuarter
    }
    // at this point, we should have column "month" like 2018M09 or column "quarter" like 2018Q3, ready for aggregation.
    dfm.na.fill(0, Seq(freq)).cache()
  }

  def proc_step2(df: DataFrame, query_filter: String, freq: String) = {
    df.filter(query_filter)
  }

  def proc_step3(df: DataFrame) = {
    df
      .groupBy("TRX_ISO_CNTRY_CD", "DIST_ID", "BAL_TYPE_ID")
      .agg(expr("sum(bal_amt) as TOTAL"))
      .withColumn("TOTAL", regexp_replace(format_number(col("TOTAL"), 2), ",", ""))
      .orderBy("TRX_ISO_CNTRY_CD", "BAL_TYPE_ID", "DIST_ID", "TOTAL")
    //    dfma.show()
  }


  def PreSink(df: DataFrame, mq: String, mode: String, freq:String) = {
    logger.info(s"=== Report for $freq $mq is being generated ...")
    val rpt_prefix = prefix_s + "_" + mode + "_" + freq.toUpperCase() + "-" + mq
    writeToCsv(df, rpt_prefix)
  }

  def writeToCsv(df: DataFrame, prefix: String) = {
    val fn = data_out + "/" + prefix + "_" +
      LocalDateTime.now().format(DateTimeFormatter.ofPattern("MM-dd-yyyy'T'HHmmss.SSS"))
    logger.info(s"=== Report $fn has been generated.")
    df
      .repartition(1)
      .write.format("csv")
      .option("header", "true")
      //      .option("quote", "")
      .mode("overwrite")
      .save(fn)
  }

  implicit class DFU(df: DataFrame) {
    def addColumnMonth() = {
      df.withColumn(C.MM, // for montly report, get year and month directly from trx_dt column
        concat(
          regexp_extract(col("trx_dt"), "(\\d+)/\\d+/(\\d{4})", 2),
          lit(C.M),
          lpad(
            regexp_extract(col("trx_dt"), "(\\d+)/\\d+/(\\d{4})", 1),
            2, "0"
          )
        )
      )
    }

    def addColumnQuarter() = {
      df.withColumn(C.QQ, // for quarterly reports, pull quarter from trx_dt.
        concat(
          regexp_extract(col("TRX_DT"), "(\\d+)/\\d+/(\\d{4})", 2),
          lit(C.Q),
          quarter(to_date(col("TRX_DT"), "M/d/y"))
        )
      )
    }
  }

}
