package com.atguigu.bigdata.spark.core.frameWork1.bean

import scala.beans.BeanProperty

/**
 * @author Yan
 * @create 2023-09-21 18:40
 * */
class UserVisitActionBean extends Serializable {
    
    @BeanProperty  var date: String = _ //用户点击行为的日期
    @BeanProperty  var user_id: Long = _ //用户的 ID
    @BeanProperty  var session_id: String = _ //Session 的 ID
    @BeanProperty  var page_id: Long = _ //某个页面的 ID
    @BeanProperty  var action_time: String = _ //动作的时间点
    @BeanProperty  var search_keyword: String = _ //用户搜索的关键词
    @BeanProperty  var click_category_id: Long = _ //某一个商品品类的 ID
    @BeanProperty  var click_product_id: Long = _ //某一个商品的 ID
    @BeanProperty  var order_category_ids: String = _ //一次订单中所有品类的 ID 集合
    @BeanProperty  var order_product_ids: String = _ //一次订单中所有商品的 ID 集合
    @BeanProperty  var pay_category_ids: String = _ //一次支付中所有品类的 ID 集合
    @BeanProperty  var pay_product_ids: String = _ //一次支付中所有商品的 ID 集合
    @BeanProperty  var city_id: Long = _ //城市 id
    
}



