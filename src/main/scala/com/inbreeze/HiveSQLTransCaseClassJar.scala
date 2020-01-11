package com.inbreeze

import java.io.{File, PrintWriter}

import scala.collection.mutable
import scala.io._
import scala.util.control.Breaks._
import scala.util.matching.Regex

/**
 * @Project FinalProject
 * @Author INBreeze
 * @Date 2020/1/10 19:45
 * @Description 将 HiveSQL 建表语句文件 转换为 CaseClass文件
 * 命令案例：java -jar HiveScalaUtil-1.0-SNAPSHOT-jar-with-dependencies.jar F:\IdeaProjects\FinalProject\ODS_Spark\src\main\resources\LogData\Hive建表_做题.txt dwd_,dws_,ads_ false
 */
//noinspection ScalaDocUnknownTag
object HiveSQLTransCaseClassJar {
  def main(args: Array[String]): Unit = {
    //1.读取文件
    val source = Source.fromFile(args(0))

    //2.处理HiveSQL
    //2.1 切割成 tableArr
    val allStr: String = source.mkString
    //println(allStr)
    val tableArr: Array[String] = allStr.split(";")
    //2.2 将 tableArr 全部转换成 样例类 String
    var tables = ""
    tableArr.foreach {
      table => {
        tables += singleTableParse(table, args(1).split(",").toList, isDropPrefix = args(2).toBoolean) + "\n"
      }
    }
    //println(tables)

    //3.输出.scala文件
    val writer = new PrintWriter(new File("caseClass.scala"))
    writer.write(tables)
    writer.close()
  }

  /**
   * 单个 table 解析，转换为 单个 Case Class 字符串
   *
   * @param str     HiveSQL单表
   * @param prefixs 表名前缀 List集合
   * @return 转换后的单个样例类 Str
   */
  def singleTableParse(str: String, prefixs: List[String], isDropPrefix: Boolean): String = {
    //0 根据表名前缀进行模式匹配获取表名 => 样例类名
    var className = (("(" + prefixs.mkString("|") + ")" + "(.*)[$a-zA-Z0-9]").r findFirstIn str).getOrElse("a" * prefixs.length)
    //println(className)
    if (isDropPrefix) {
      prefixs.foreach(
        prefix => {
          if (className.contains(prefix))
            className = className.replace(prefix, "")
        }
      )
    }

    //1 去除 每行行首空格，再拼接为一行数据
    var trimedStr = ""
    str.split("\r\n").foreach {
      trimedStr += _.trim
    }
    //println(trimedStr)

    //2 根据 两个 () 分别获取 普通列 及 分区列 以及 不需要的 tableProperties
    //有序 属性列名+类型 存储容器
    val map = mutable.LinkedHashMap[String, String]()

    //对 decimal(x,y) 进行预处理转换，避免错误匹配"()"
    //防止贪婪算法 "?)" 只匹配最近的一个")"
    val iterator: Regex.MatchIterator = "\\([\\s\\S]*?\\)".r findAllIn trimedStr.replaceAll("decimal\\(.*?\\)", "BigDecimal")

    //只取 普通列 分区列 2个"()"
    var ParenthesesCount = 1
    breakable(
      iterator.foreach {
        Parentheses =>
          //println(str)
          if (ParenthesesCount == 3) break
          Parentheses.drop(1).dropRight(1).split(",").foreach {
            fieldRow =>
              val fieldAndType = fieldRow.replaceAll("`", "").split(" ")
              //println(fieldAndType(0) + ":" + fieldAndType(1))
              fieldAndType(1) match {
                case "int" => map.put(fieldAndType(0), "Int")
                case "string" => map.put(fieldAndType(0), "String")
                case "timestamp" => map.put(fieldAndType(0), "String")
                case "BigDecimal" => map.put(fieldAndType(0), "BigDecimal")
                case _ => ""
              }
          }
          ParenthesesCount += 1
      }
    )
    //map.iterator.foreach(println)

    //3 拼接单个表转换样例类结果
    var result = "case class " + className + "(\n"
    for (elem <- map.iterator) {
      result += elem._1 + ": " + elem._2 + ",\n"
    }
    result = result.dropRight(2) + "\n)"
    //println(result)

    result
    //笔记
    //注1：Map相关
    //不能写 mutable.Map[String,String] 这个泛型不起作用，而且不能put，.+也有问题
    //mutable.map + (arr(0)->"Int") 这样调用不会加入任何新元素
    //LinkedHashMap为记录插入有序Map，其他Map均为无序输出。
    //注2：正则相关
    //"[]"匹配范围多个, "."匹配除"\r\n"之外的任何单个字符,"\\s"匹配空符号[\t\r\n\f],"\\S"匹配非空符号
    //"*"重复零次或更多次,"?"重复零次或一次
    //注3：List Array Seq
    //Seq
    //IndexedSeq LinerSeq
    //Array      List
    //List与Array 在Scala中属于不同特质下的两个类
    //IndexedSeq通过索引来查找和定位，因此速度快，如String就是一个索引集合，通过索引即可定位
    //LinearSeq是线性的，即存在头尾的概念，这种数据结构一般是通过遍历来查找
  }
}
