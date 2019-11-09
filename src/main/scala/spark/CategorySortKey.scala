package spark

import scala.math.Ordered
import java.io.Serializable

/**
  * 品类二次排序key
  *
  * 封装你要进行排序算法需要的几个字段；点击次数、下单次数和支付次数
  * 实现Ordered接口要求的几个方法
  *
  * 跟其他key相比，如何来判定大于、大于等于、小于、小于等于
  * 依次使用三个次数进行比较，如果某一个相等，那么就比较下一个
  *
  * 自定义的二次排序key，必须要实现serializable接口，表明是可以序列化 的，否则会报错
  */

class CategorySortKey extends Ordered[CategorySortKey] with Serializable {
    private[CategorySortKey] val serialVersionUID = -565992549650791884L
    private[CategorySortKey] var clickCount: Long = _
    private[CategorySortKey] var orderCount: Long = _
    private[CategorySortKey] var payCount: Long = _

    // 辅助构造函数
    def this(clickCount: Long, orderCount: Long, payCount: Long) {
        this()
        this.clickCount = clickCount
        this.orderCount = orderCount
        this.payCount = payCount
    }

    def getClickCount: Long = {
        this.clickCount
    }

    def setClickCount(clickCount: Long): Unit = {
        this.clickCount = clickCount
    }

    def getOrderCount: Long = {
        this.orderCount
    }

    def setOrderCount(orderCount: Long): Unit = {
        this.orderCount = orderCount
    }

    def getPayCount: Long = {
        this.payCount
    }

    def setPayCount(payCount: Long): Unit = {
        this.payCount = payCount
    }

    override def >(that: CategorySortKey): Boolean = {
        if (clickCount > that.getClickCount) {
            return true
        } else if (clickCount == that.getClickCount && orderCount > that.getOrderCount) {
            return true
        } else if (clickCount == that.getClickCount && orderCount == that.getOrderCount
            && payCount > that.payCount) {
            return true
        }
        false
    }

    override def >=(that: CategorySortKey): Boolean = {
        if (this > that) {
            return true
        } else if (clickCount == that.getClickCount &&
            orderCount == that.getOrderCount &&
            payCount == that.getPayCount) {
            return true
        }
        false
    }

    override def <(that: CategorySortKey): Boolean = {
        if (clickCount < that.getClickCount) return true
        else if ((clickCount == that.getClickCount) && orderCount < that.getOrderCount) return true
        else if ((clickCount == that.getClickCount) &&
            (orderCount == that.getOrderCount) && payCount < that.getPayCount) return true
        false
    }

    override def <=(that: CategorySortKey): Boolean = {
        if (this < that) return true
        else if (clickCount == that.getClickCount && orderCount == that.getOrderCount
            && payCount == that.getPayCount) return true
        false
    }

    override def compare(that: CategorySortKey): Int = {
        if (clickCount - that.getClickCount != 0) return (clickCount - that.getClickCount).toInt
        else if (orderCount - that.getOrderCount != 0) return (orderCount - that.getOrderCount).toInt
        else if (payCount - that.payCount != 0) return (payCount - that.getPayCount).toInt
        0
    }

    override def compareTo(that: CategorySortKey): Int = {
        this.compare(that)
    }
}
