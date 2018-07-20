package com.scala.krishna

class myException extends java.lang.Exception() {}
object hiveEtl {
  
import org.slf4j.LoggerFactory
import java.util.{Calendar, Date}
import org.apache.spark._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col




val logger = LoggerFactory.getLogger(hiveEtl.getClass)
  var ErrorLogging = new Array[String](6)  
  
  
    /** Taking args as input and returing Map As output **/
  def argValidation(args: Array[String]) {
    logger.info("Input Arguments are :" + args);
    val arr =  args.map(x => x.trim());
    if(arr.length == 1){
      logger.info("No.Of Arguments Passed ..!!!", +arr.length);
    }
  }
  
  def p_manage_logging(input:Array[String]){
    input(0) = Calendar.getInstance().getTime().toString()
    println(input.mkString(","))
    logger.info("Program:"+input.mkString(","))
  }
  
  
  /** Null Handling of any type variable ,However, because filter just takes a Boolean value,***/
  
   def isAnyNull(value:Any*):Boolean = {
    value.filter(value => value == null).size > 0
  }
   
   def iwork_join(sc:SparkContext,hiveContext:HiveContext,epmData:DataFrame,cntry:String): DataFrame = {
     val iwrkRead = hiveContext.sql(s"""select source_porfolio_cd,source_data_loc_cd,business_lines,product_lines,sub_product_lines,entity,cntry_cd as iwork_cntry_cd FROM nrd_app_frdv.IWORK WHERE Product_lines in ('CMU Long Term','CMU Short Term') and cntry_cd='$cntry' """)
     val lcinOut = iwrkRead.join(epmData,iwrkRead.col("source_porfolio_cd").equalTo(epmData.col("lcin")),"inner")
     println("output of the iwork")
     lcinOut.show()
     return lcinOut
   }
   
   def murex_cmu(sc: SparkContext, hiveContext: HiveContext,biz_date:String , country:String){
     try{
       println("value is :" +country)
     val epmConsolidated = hiveContext.sql(s"""Select * from nrd_app_frdv.epm_consolidated_ledger where sourcedataloccd='$country' and businessdt='$biz_date' and primarysourcesyscd='TQSP' """)
     println("Printing the EpmDetails")
     epmConsolidated.show()
     val lcins = iwork_join(sc,hiveContext,epmConsolidated,country)
     println("selecting the cols")
     lcins.select(col("lcin"),col("sourcedataloccd")).show()
     
     } catch{
       /** Writing code to throw our own execption and throwing default exepection**/
       case t: Throwable => {
         ErrorLogging(0)="murex_cmu:"
         ErrorLogging(1)="When Accessing Epm Consolidate"
         ErrorLogging(2)=s"$biz_date" 
         p_manage_logging(ErrorLogging)
       }
       case t:myException => {
         ErrorLogging(0)="murex_cmu"
         ErrorLogging(1)="Error"
         ErrorLogging(2)="UserDefined Error"
          p_manage_logging(ErrorLogging)
       }
     }
     
      
    }
   
   /** Scala Compiler Starts from here **/
   
  def main(args: Array[String]) {
        println("Usage:  [tablename]")
  
        
  val businessDate = args(0)
  val country = args(1)
  
  println("businessdt:"+args(0))
  println("country:"+args(1))
  
  if (isAnyNull(businessDate)) {
    throw new RuntimeException("The Give Input Argument is Null");
  }
   ErrorLogging(0) = "Initiating program - calling main()"
   p_manage_logging(ErrorLogging)
      
       val conf = new SparkConf().setAppName("ScalaProgramTest")
      .set("spark.shuffle.spill.compress", "true")
      .set("spark.shuffle.compress", "true")
      
       val usage = s""" Usage for $businessDate: <CountryCode>""".stripMargin
       
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)
          hiveContext.setConf("hive.exec.dynamic.partition", "true")
          hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
          hiveContext.setConf("spark.sql.parquet.filterPushdown", "false")
          hiveContext.setConf("spark.shuffle.spill.compress", "true")
          hiveContext.setConf("spark.shuffle.compress", "true")
          hiveContext.setConf("spark.sql.tungsten.enabled", "false")
          murex_cmu(sc,hiveContext,businessDate,country)
          sc.stop()
  }
  
 
  
}