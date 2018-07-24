package com.futhead.scala.sql

import org.apache.hadoop.hive.ql.optimizer.spark.SparkSkewJoinProcFactory.SparkSkewJoinJoinProcessor
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/7/9.
  */
object UserActiveDegreeAnalyze {

  case class UserActionLog(logId: Long, userId: Long, actionTime: String, actionType: Long, purchaseMoney: Double)
  case class UserActionLogVO(logId: Long, userId: Long, actionValue: Long)
  case class UserActionLogWithPurchaseMoneyVo(logId: Long, userId: Long, purchaseMoney: Double)

  def main(args: Array[String]): Unit = {
    val startDate = "2016-09-01"
    val endDate = "2016-11-01"

    val spark = SparkSession
      .builder()
      .appName("UserAcitveDegreeAnalyze")
      .master("local")
      .getOrCreate()

    //导入spark隐式转换
    import spark.implicits._
    //导入spark sql的functions
    import org.apache.spark.sql.functions._

    val userBaseInfo = spark.read.json("E:\\big-data\\data\\user_base_info.json")
    val userActionLog = spark.read.json(("E:\\big-data\\data\\user_action_log.json"))

//    userBaseInfo.show(5)
//    userActionLog.show(5)

    // 统计指定时间范围内的访问次数最多的10个用户
//    userActionLog
//      .filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 0")
//      .join(userBaseInfo, userBaseInfo("userId") === userActionLog("userId"))
//      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
//      .agg(count(userActionLog("logId")).alias("actionCount"))
//      .sort($"actionCount".desc)
//      .limit(10)
//      .show()

    // 获取指定时间范围内购买金额最多的10个用户
//    userActionLog
//      .filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 1")
//      .join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId"))
//      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
//      .agg(round(sum(userActionLog("purchaseMoney")), 2).alias("totalPurchaseMoney"))
//      .sort($"totalPurchaseMoney".desc)
//      .limit(10)
//      .show()

    //统计最近一个周期相对上一个周期访问次数增长最多的10个用户
//    val userActionLogInFirstPeriod = userActionLog.as[UserActionLog]
//      .filter("actionTime >= '2016-10-01' and actionTime <= '2016-10-31' and actionType = 0")
//      .map(userActionLogEntry => UserActionLogVO(userActionLogEntry.logId, userActionLogEntry.userId, 1))
//    val userActionLogInSecondPeriod = userActionLog.as[UserActionLog]
//      .filter("actionTime >= '2016-01-01' and actionTime <= '2016-09-30' and actionType = 0")
//      .map(userActionLogEntry => UserActionLogVO(userActionLogEntry.logId, userActionLogEntry.userId, -1))
//    val userActionLogDS = userActionLogInFirstPeriod.union(userActionLogInSecondPeriod)
//
//    userActionLogDS.join(userBaseInfo, userActionLogDS("userId") === userBaseInfo("userId"))
//      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
//      .agg(sum(userActionLogDS("actionValue")).alias("actionIncr"))
//      .sort($"actionIncr".desc)
//      .limit(10)
//      .show()

    //统计最近一个周期相比上一个周期购买金额增长最多的10个用户
//    val userAcitonLogInFirstPeriod = userActionLog.as[UserActionLog]
//      .filter("actionTime >= '2016-10-01' and actionTime <= '2016-10-31' and actionType = 1")
//      .map(userActionLogEntry => UserActionLogWithPurchaseMoneyVo(userActionLogEntry.logId, userActionLogEntry.userId, userActionLogEntry.purchaseMoney))
//    val userActionLogInSecondPeriod = userActionLog.as[UserActionLog]
//      .filter("actionTime >= '2016-01-01' and actionTime <= '2016-09-30' and actionType = 1")
//      .map(userActionLogEntry => UserActionLogWithPurchaseMoneyVo(userActionLogEntry.logId, userActionLogEntry.userId, -userActionLogEntry.purchaseMoney))
//
//    val userActionLogWithPurchaseMoneyDS = userAcitonLogInFirstPeriod.union(userActionLogInSecondPeriod)
//    userActionLogWithPurchaseMoneyDS.show()
//    userActionLogWithPurchaseMoneyDS.join(userBaseInfo, userActionLogWithPurchaseMoneyDS("userId") === userBaseInfo("userId"))
//      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
//      .agg(round(sum(userActionLogWithPurchaseMoneyDS("purchaseMoney")), 2).alias("purchaseMoneyIncr"))
//      .sort($"purchaseMoneyIncr".desc)
//      .limit(10)
//      .show()

    //用户活跃度
    //统计指定注册时间范围内头7天访问次数最高的10个用户
    userActionLog.join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId"))
      .filter(userBaseInfo("registTime") >= "2016-10-01"
        && userBaseInfo("registTime") <= "2016-10-31"
        && userActionLog("actionTime") >= userBaseInfo("registTime")
        && userActionLog("actionTime") <= date_add(userBaseInfo("registTime"), 7)
        && userActionLog("actionType") === 0)
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(count(userActionLog("logId")).alias("actionCount"))
      .sort($"actionCount".desc)
      .limit(10)
      .show()

    //统计指定注册时间范围内头7天消费最高的10个用户
    userActionLog.join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId"))
      .filter(userBaseInfo("registTime") >= "2016-10-01"
        && userBaseInfo("registTime") <= "2016-10-31"
        && userActionLog("actionTime") >= userBaseInfo("registTime")
        && userActionLog("actionTime") <= date_add(userBaseInfo("registTime"), 7)
        && userActionLog("actionType") === 1)
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(round(sum(userActionLog("purchaseMoney")), 2).alias("purchaseMoneyTotal"))
      .sort($"purchaseMoneyTotal".desc)
      .limit(10)
      .show()

  }

}
