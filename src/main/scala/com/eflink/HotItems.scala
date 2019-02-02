package com.eflink

// Ref:
// http://wuchong.me/blog/2018/11/07/use-flink-calculate-hot-items/
// https://github.com/uybhatti/FlinkCheckpoint/blob/master/flink-tests/src/test/scala/org/apache/flink/api/scala/io/CsvInputFormatTest.scala
// https://github.com/masato/streams-flink-scala-examples/blob/master/src/main/scala/com/github/masato/streams/flink/App.scala

import org.apache.flink.api.java.io.PojoCsvInputFormat
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConversions._
import java.net.URL
import java.io.File
import java.sql.Timestamp

import scala.collection.mutable.ListBuffer
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.tuple.Tuple1
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector



// equal java pojo
// cooperate with Java frameworks, you still need no-argu constructor and setters
/** 用户行为数据结构 **/
class POJOItem(var userId: Long, var itemId: Long, var categoryId: Long, var behavior: String, var timestamp: Long) {
  def this(){
    this(0, 0, 0, "", 0)
  }
}

object HotItems {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // deal with event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // set parallelism
    env.setParallelism(1)

    // read resource file as input source
    val fileUrl: URL = this.getClass.getClassLoader.getResource("resources/UserBehavior.csv")
    val filePath: Path = Path.fromLocalFile(new File(fileUrl.toURI))

    // PojoTypeInfo
    val typeInfo: PojoTypeInfo[POJOItem] = createTypeInformation[POJOItem].asInstanceOf[PojoTypeInfo[POJOItem]]
    val pojoField = Array("userId", "itemId", "categoryId", "behavior", "timestamp")
    // create PojoCsvInputFormat
    val format: PojoCsvInputFormat[POJOItem] = new PojoCsvInputFormat[POJOItem](filePath, typeInfo, pojoField)
    // 创建数据源，得到POJOItem 类型的DataStream
    val source = env.createInput(format)

    // 抽取事件生成watermark
    val timedData = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[POJOItem] {
      override def extractAscendingTimestamp(t: POJOItem): Long = t.timestamp * 1000
    })
    // 过滤只有点击行为的数据
    val pvData = timedData.filter(_.behavior.equals("pv"))

    val windowedData = pvData.keyBy("itemId")
        .timeWindow(Time.minutes(60), Time.minutes(5))
        .aggregate(new CountAgg(), new WindowResultFunction())

    windowedData.keyBy("windowEnd")
      .process(new TopNHotItems(3))
      .print()

    env.execute("Hot Items Job")

  }

}


class CountAgg extends AggregateFunction[POJOItem, Long, Long]{
  override def createAccumulator(): Long = 0

  override def add(in: POJOItem, acc: Long): Long = acc + 1

  override def merge(acc: Long, acc1: Long): Long = acc + acc1

  override def getResult(acc: Long): Long = acc
}


class WindowResultFunction extends ProcessWindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
  override def process(key: Tuple, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val count: Long = elements.iterator.next
    out.collect(ItemViewCount(itemId, context.window.getEnd, count))
  }

}

/** 商品点击量(窗口操作的输出类型) */
case class ItemViewCount(val itemId: Long, val windowEnd: Long, var viewCount: Long){

}


//object ItemViewCount{
//  def apply(itemId: Long, windowEnd: Long, viewCount: Long): ItemViewCount = new ItemViewCount(itemId, windowEnd, viewCount)
//}


/** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 **/
class TopNHotItems extends KeyedProcessFunction[Tuple, ItemViewCount, String]{
  private var topSize: Long = 0L
  // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
  private var itemState: ListState[ItemViewCount] = null
  def this(topSize: Long) {
    this()
    this.topSize = topSize
  }
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val itemsStateDesc = new ListStateDescriptor("itemState-state", ItemViewCount.getClass)
    itemState = getRuntimeContext.getListState(itemsStateDesc).asInstanceOf[ListState[ItemViewCount]]
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每条数据都保存到状态中
    itemState.add(value)
    // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
    ctx.timerService.registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //super.onTimer(timestamp, ctx, out)
    // 获取收到的所有商品点击
    val allItems = ListBuffer.empty[ItemViewCount]
    for(item <- itemState.get){
      allItems.append(item)
    }
    // 提前清除状态中的数据，释放空间
    itemState.clear()
    // 按照点击量从大到小排序
    val allSortedItems = allItems.sortWith((i1: ItemViewCount, i2: ItemViewCount) => (i1.viewCount-i2.viewCount)>0)
    // 将排名信息格式化成 String, 便于打印
    val result = new StringBuilder
    result.append("====================================\n")
    result.append("时间： ").append(new Timestamp(timestamp-1)).append("\n")
    for(i <- 1 to topSize.asInstanceOf[Int]){
      val currentItem = allSortedItems(i-1)
      result.append("No").append(i).append(":")
        .append("  商品ID=").append(currentItem.itemId)
        .append("  浏览量=").append(currentItem.viewCount)
        .append("\n")
    }
    result.append("====================================\n\n")
    out.collect(result.toString())
  }
}


