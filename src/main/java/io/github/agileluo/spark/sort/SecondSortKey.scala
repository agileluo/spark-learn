package io.github.agileluo.spark.sort

class SecondSortKey(val first:Int, val second:Int) extends Ordered[SecondSortKey] with Serializable {
    def compare(that: SecondSortKey): Int = {
      val firstCompare = this.first - that.first;
      if(firstCompare != 0){
        firstCompare
      }else{
        this.second - that.second
      }
    }
}