# 作业三：实现 Compact table command

## 作业要求

- 添加 compact table 命令，用于合并小文件，例如表 test1 总共有 50000 个文件，每个 1MB，通过该命令，合成为 500 个文件，每个约 100MB。

- 基本要求是完成以下功能：COMPACT TABLE test1 INTO 500 FILES；

  如果添加 partitionSpec，则只合并指定的 partition 目录的文件；

  如果不加 into fileNum files，则把表中的文件合并成 128MB 大小。

## 源码版本

```
版本：https://github.com/apache/spark/tree/branch-3.3
```

## 思路

先计算分区----> 创建中间表存储原数据-------->改表名

```
val tablePartitionsNumber = Math.max((sparkSession.sessionState
      .executePlan(table.logicalPlan)
      .optimizedPlan.stats.sizeInBytes >> 17).toInt, 1)
```

```
    table.repartition(tablePartitionsNumber)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tmpTableName)

    val tmpTable = sparkSession.table(tmpTableName)

    tmpTable
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableId.table)

    sparkSession.sql(s"DROP TABLE $tmpTableName ;")
```



## 代码修改

### **修改SqlBase.g4**

路径sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4  

```tex
@@ -244,6 +244,8 @@
     | SET .*?                                                          #setConfiguration
     | RESET configKey                                                  #resetQuotedConfiguration
     | RESET .*?                                                        #resetConfiguration
+    | COMPACT TABLE target=tableIdentifier partitionSpec?
+    (INTO fileNum=INTEGER_VALUE identifier)?                           #compactTable
     | unsupportedHiveNativeCommands .*?                                #failNativeCommand
     ;
 
@@ -1103,6 +1105,7 @@
     | EXTRACT
     | FIELDS
     | FILEFORMAT
+    | FILES
     | FIRST
     | FOLLOWING
     | FORMAT
@@ -1347,6 +1350,7 @@
     | FILTER
     | FIELDS
     | FILEFORMAT
+    | FILES
     | FIRST
     | FOLLOWING
     | FOR
@@ -1602,6 +1606,7 @@
 FIELDS: 'FIELDS';
 FILTER: 'FILTER';
 FILEFORMAT: 'FILEFORMAT';
+FILES: 'FILES';
 FIRST: 'FIRST';
 FOLLOWING: 'FOLLOWING';
 FOR: 'FOR';
```



### 修改SparkSqlParser.scala

 sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala

```scala
  /**
   *
   * @param ctx the parse tree
   *
   */
  override def visitCompactTable(ctx: CompactTableContext): LogicalPlan = withOrigin(ctx) {
    val tableName = visitTableIdentifier(ctx.tableIdentifier())
    val fileNum = if (ctx.INTEGER_VALUE() != null) {
      Some(ctx.INTEGER_VALUE().getText)
    } else {
      None
    }
    CompactTableCommand(tableName, fileNum)
  }
```



### 新增CompactTableCommand

sql\core\src\main\scala\org\apache\spark\sql\execution\command

```scala
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType

/**
 * Compact Table
 *
 * @param tableId table Id
 */
case class CompactTableCommand(tableId: TableIdentifier, fileNumber: Option[String]) extends
  LeafRunnableCommand {
  override val output: Seq[Attribute] =
    Seq(AttributeReference("compact", StringType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // get Table by tableIdentifier
    val table = sparkSession.table(tableId)

    // recalculate partition number
    var tablePartitionsNumber = fileNumber match {
      case None => (sparkSession.sessionState
        .executePlan(table.logicalPlan)
        .optimizedPlan.stats.sizeInBytes >> 7).toInt
      case Some(value) => value.toInt
    }
    // ( Number of partitions (0) must be positive)
    tablePartitionsNumber = Math.max(tablePartitionsNumber, 1)

    val tmpTableName = tableId.table + "_tmp"

    // rewrite the files base on the recalculated partition number
    // FYI:
    // Here we introduce temp table to solve
    // "Cannot overwrite table that is also being read from" issue

    table.repartition(tablePartitionsNumber)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tmpTableName)

    val tmpTable = sparkSession.table(tmpTableName)

    tmpTable
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableId.table)

    sparkSession.sql(s"DROP TABLE $tmpTableName ;")

    Seq(Row(s"Compact table ${tableId.table} finished"))
  }
}
```

## 测试

- 合并文件逻辑时将 `128MB` 配置改为 `128B`重新编译
- 创建测试表并插入测试数据

> ```
> CREATE TABLE block_list(accountType INT, city string);
> INSERT INTO block_list VALUES 
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3"),
> (1, "BJ1"),(2, "BJ2"),(3, "BJ3");
> ```

- 查看表文件

```
➜  spark git:(branch-3.3) ✗ ls -lh spark-warehouse/block_list
total 64
-rwxr-xr-x  1 xujingtian  staff    90B May 21 17:15 part-00000-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 xujingtian  staff    90B May 21 17:15 part-00001-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 xujingtian  staff    90B May 21 17:15 part-00002-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 xujingtian  staff    90B May 21 17:15 part-00003-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 xujingtian  staff    90B May 21 17:15 part-00004-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 xujingtian  staff    90B May 21 17:15 part-00005-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 xujingtian  staff    90B May 21 17:15 part-00006-0067344d-07ec-4836-878f-549dea59047b-c000
-rwxr-xr-x  1 xujingtian  staff    90B May 21 17:15 part-00007-0067344d-07ec-4836-878f-549dea59047b-c000
```

### 用例1-不指定 fileNumber

- spark-sql执行`COMPACT TABLE` 不指定 fileNumber

```
spark-sql> compact table block_list;
Compact table block_list finished
```

- 查看表文件

```
➜  spark git:(branch-3.3) ✗ ls -lh spark-warehouse/block_list
total 40
-rw-r--r--  1 xujingtian  staff     0B May 21 17:45 _SUCCESS
-rw-r--r--  1 xujingtian  staff   803B May 21 17:45 part-00000-11db45d9-05f5-45c3-b238-c52f48a8a4b4-c000.snappy.parquet
-rw-r--r--  1 xujingtian  staff   803B May 21 17:45 part-00001-11db45d9-05f5-45c3-b238-c52f48a8a4b4-c000.snappy.parquet
-rw-r--r--  1 xujingtian  staff   803B May 21 17:45 part-00002-11db45d9-05f5-45c3-b238-c52f48a8a4b4-c000.snappy.parquet
-rw-r--r--  1 xujingtian  staff   803B May 21 17:45 part-00003-11db45d9-05f5-45c3-b238-c52f48a8a4b4-c000.snappy.parquet
-rw-r--r--  1 xujingtian  staff   803B May 21 17:45 part-00004-11db45d9-05f5-45c3-b238-c52f48a8a4b4-c000.snappy.parquet
```

### 用例2 - 指定 fileNumber 

- spark-sql执行`COMPACT TABLE` 指定 fileNumber 为正整数

```
spark-sql> compact table block_list into 2 files;
Compact table block_list finished
```

- 查看表文件

```
➜  spark git:(branch-3.3) ✗ ls -lh spark-warehouse/block_list             
total 16
-rw-r--r--  1 xujingtian  staff     0B May 21 18:26 _SUCCESS
-rw-r--r--  1 xujingtian  staff   801B May 21 18:26 part-00000-84d417a4-ad51-40bd-8131-3760b92ddf0b-c000.snappy.parquet
-rw-r--r--  1 xujingtian  staff   801B May 21 18:26 part-00001-84d417a4-ad51-40bd-8131-3760b92ddf0b-c000.snappy.parquet
```
