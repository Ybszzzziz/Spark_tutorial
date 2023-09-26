package com.atguigu.bigdata.spark.util
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource
import com.alibaba.druid.pool.DruidDataSourceFactory
import jodd.util.PropertiesUtil
import org.apache.spark.deploy.yarn.config
/**
 * @author Yan
 * @create 2023-09-26 19:01
 * */
object JDBCUtil {
    //初始化连接池
    var dataSource: DataSource = init()
    
    //初始化连接池方法
    def init(): DataSource = {
        val properties = new Properties()
        properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
        properties.setProperty("url", "jdbc:mysql://hadoop102:3306/spark-streaming?useUnicode=true&" +
                "characterEncoding=UTF-8")
        properties.setProperty("username", "root")
        properties.setProperty("password", "Ybs123123.")
        properties.setProperty("maxActive", "100")
        DruidDataSourceFactory.createDataSource(properties)
    }
    
    //获取 MySQL 连接
    def getConnection: Connection = {
        dataSource.getConnection
    }
    
    //执行 SQL 语句,单条数据插入
    def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int
    = {
        var rtn = 0
        var pstmt: PreparedStatement = null
        try {
            connection.setAutoCommit(false)
            pstmt = connection.prepareStatement(sql)
            if (params != null && params.length > 0) {
                for (i <- params.indices) {
                    pstmt.setObject(i + 1, params(i))
                }
            }
            rtn = pstmt.executeUpdate()
            connection.commit()
            pstmt.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        rtn
    }
    
    //执行 SQL 语句,批量数据插入
    def executeBatchUpdate(connection: Connection, sql: String, paramsList:
    Iterable[Array[Any]]): Array[Int] = {
        var rtn: Array[Int] = null
        var pstmt: PreparedStatement = null
        try {
            connection.setAutoCommit(false)
            pstmt = connection.prepareStatement(sql)
            for (params <- paramsList) {
                if (params != null && params.length > 0) {
                    for (i <- params.indices) {
                        pstmt.setObject(i + 1, params(i))
                    }
                    pstmt.addBatch()
                }
            }
           rtn = pstmt.executeBatch()
            connection.commit()
            pstmt.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        rtn
    }
    
    //判断一条数据是否存在
    def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
        var flag: Boolean = false
        var pstmt: PreparedStatement = null
        try {
            pstmt = connection.prepareStatement(sql)
            for (i <- params.indices) {
                pstmt.setObject(i + 1, params(i))
            }
            flag = pstmt.executeQuery().next()
            pstmt.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        flag
    }
    
    //获取 MySQL 的一条数据
    def getDataFromMysql(connection: Connection, sql: String, params: Array[Any]):
    Long = {
        var result: Long = 0L
        var pstmt: PreparedStatement = null
        try {
            pstmt = connection.prepareStatement(sql)
            for (i <- params.indices) {
                pstmt.setObject(i + 1, params(i))
            }
            val resultSet: ResultSet = pstmt.executeQuery()
            while (resultSet.next()) {
                result = resultSet.getLong(1)
            }
            resultSet.close()
            pstmt.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        result
    }
    
}
