package cn.wonhigh.exercise


/**
  * 通过自定排序规则， 实现二次排序功能
  */
class SecondarySortKey(val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if (this.first - that.first != 0) { //如果第一列不等，直接返回
      this.first - that.first
    } else { //如果第一列相等，比较第二列
      this.second - that.second
    }
  }
}
