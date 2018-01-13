package com.sparkTutorial.myCodes

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object siftCandies {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("candies").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val candyFirst = sc.textFile("in/candy-data.csv")

    //Enter the qualifications of the candy you want

    val hasChocolate=1.0
    val fruity=0.0
    val hasCaramel=0.0
    val isHard=0.0
    val isBar=1.0

    val checkConditions=(chocolate:Double,fruit:Double,caramel:Double,hardness:Double,Bar:Double) =>{

      chocolate==hasChocolate && fruit==fruity && caramel==hasCaramel && hardness==isHard && Bar==isBar
    }

    val printGreeting=(chocolate:Double,fruit:Double,caramel:Double,hardness:Double,Bar:Double) =>{

      println("Hey buddy!")
      var infoPhrase="You selected a candy that has : "

      if(chocolate==1.0){
        infoPhrase += "chocolate "
      }

      if(fruit==1.0){
        infoPhrase += "fruit "
      }

      if(caramel==1.0){
        infoPhrase += "caramel "
      }

      if(hardness==1.0){
        infoPhrase += " is hard "
      }

      if(Bar==1.0){
        infoPhrase += "and is a bar "
      }

      println(infoPhrase)



    }


    val cleanedLines = candyFirst.filter(line => !line.contains("chocolate"))

    val candyInfo = cleanedLines.map(line => (line.split(",")(0), line.split(",")(1).toDouble,line.split(",")(2).toDouble,line.split(",")(3).toDouble,
      line.split(",")(7).toDouble,line.split(",")(8).toDouble,line.split(",")(10).toDouble,line.split(",")(11).toDouble))

    val candyIwant=candyInfo.filter(line=> checkConditions(line._2,line._3,line._4,line._5,line._6))
    val candyNames=candyIwant .map(line=>line._1)

    val candySugarTotal=candyIwant .map(line=>line._7).reduce((x,y)=> x + y)
    val candyCount=candyIwant.count()
    val averageSugar=candySugarTotal/candyCount

    val candyPriceTotal=candyIwant .map(line=>line._8).reduce((x,y)=> x + y)
    val averagePriceRange= candyPriceTotal/candyCount


    printGreeting(hasChocolate,fruity,hasCaramel,isHard,isBar)
    println("Your options are: ")
    for (name <- candyNames) println(name )
    println("Average sugar percentage: ")
    println(averageSugar)
    println("Average price percentage: ")
    println(averagePriceRange)







  }


}
