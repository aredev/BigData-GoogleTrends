package org.rubigdata

import nl.surfsara.warcutils.WarcInputFormat
import org.jwat.warc.{WarcConstants, WarcRecord} 
import org.apache.hadoop.io.LongWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.{Logging, SparkConf}
import java.io.IOException;
import org.jsoup.Jsoup;
import java.io.InputStreamReader;
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Cells extends java.io.Serializable{

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Google Trends").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.getConf.toDebugString
    
    val warcs = sc.newAPIHadoopFile(
                  "/data/public/common-crawl/crawl-data/CC-MAIN-2016-07/segments/*/warc/*.warc.gz",
                  classOf[WarcInputFormat],               // InputFormat
                  classOf[LongWritable],                  // Key
                  classOf[WarcRecord]                     // Value
        )
    
    

    val contents = warcs.filter{ _._2.header.warcTypeIdx == 2  }.             //Check if it is a response
                   filter{ _._2.getHttpHeader().contentType != null}.        //Check if the ContentType is not Null, very important!
                   filter{ _._2.getHttpHeader().contentType.startsWith("text/html") }.   //Check if the ContentType is text/html
                   map{wr => (wr._2.header.warcTargetUriStr, getContent(wr._2))}         //Get the content
    .cache()

    val stats = contents.map{
          content => computeOccurence(content._2, "deadpool")   //Term for which we want to see how it occurs in the crawl
        }.reduce{
          (a,b) => (a._1 + b._1, a._2 + b._2)
    }
	
    val res = (stats._1.toFloat/stats._2.toFloat)*100

    println("This is the occurence of deadpool on the crawl %f".format(res))

  }
  
  //Function to get the content of a WarcRecord
  def getContent(record: WarcRecord):String = {
      val cLen = record.header.contentLength.toInt
      //val cStream = record.getPayload.getInputStreamComplete()
      val cStream = record.getPayload.getInputStream()
      val content = new java.io.ByteArrayOutputStream();
  
      val buf = new Array[Byte](cLen)
  
      var nRead = cStream.read(buf)
      while (nRead != -1) {
        content.write(buf, 0, nRead)
        nRead = cStream.read(buf)
      }
  
      cStream.close()
  
      return content.toString("UTF-8")
  }

  //Function to count the number of occurences of needle in haystack
  def countSubstrings(needle: String, haystack: String): Int = {
      var lastIndex = 0
      var count = 0;
    
      while (lastIndex != -1){
        
        lastIndex = haystack.indexOf(needle,lastIndex)      
        
        if(lastIndex != -1){
          count = count+1
          lastIndex += needle.length()
        }
      }
    return count
  }

  //Returns how often a term occurs given the content of the page
  def computeOccurence(content: String, term: String): (Int, Int) = {
    try {
      val body = Jsoup.parse(content).text().replaceAll("[^A-Za-z]", " ")
      val total = body.split(" +").length
      val needle = countSubstrings(term.toLowerCase(), body.toLowerCase())
      return (needle, total)
    }
    catch {
      case e: Exception => throw new IOException("Caught exception processing input row ", e)
    }
  }

}