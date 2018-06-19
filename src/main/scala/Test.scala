import java.util.regex.Pattern

/**
  * Created by Kratos on 2018/6/6.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val regex = "\\sfood_id:(\\d+)"
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher("original_price:  sku_id:290121461569  name:土豆肉丝饭  weight:0  virtual_food_id:648287346  pinyin_name:tudourousifan  restaurant_id:156380824  food_id:673595177  packing_fee:1  recent_rating:4  promotion_stock:-1  price:14  sold_out:False  recent_popularity:14  is_essential:False  item_id:250623906625  checkout_mode:1  specs:    name:规格  value:土豆肉丝饭      partial_reduce_activity_id:  stock:783\",\n\"original_price:  sku_id:-10000  name:土豆肉丝饭  weight:0  virtual_food_id:648287346  pinyin_name:tudourousifan  restaurant_id:156380824  food_id:-10000  packing_fee:1  recent_rating:4  promotion_stock:-1  price:14  sold_out:False  recent_popularity:0  is_essential:False  item_id:250623906625  checkout_mode:1  specs:    name:规格  value:默认      partial_reduce_activity_id:  stock:0")
    while (matcher.find()) {
      println(matcher.group(1))
    }
  }
}
