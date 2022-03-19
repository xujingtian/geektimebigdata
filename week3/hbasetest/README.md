### 部署到服务器

上传运行文件hbasetest-1.0-xujingtian.jar到服务器的

### 启用HBase环境变量

```
source env.sh
```

### 运行Hbase程序

```
# 运行jar程序
hadoop jar ./hbasetest-1.0-xujingtian.jar
```

### 程序运行过程

第一步：连接hbase服务器emr-worker-2.cluster-285604:2181

第二步：建表zhanghui:student，包含3个列蔟name、info、score

第三步：插入题中给出的4组数据，RowKey从1-4

第四步：插入name为张煇、info:student_id为G20210607040077的1组数据，Rowkey为5

第五步：查找Roweky为5的数据

第六步：删除Rowkey为1的数据

第七步：删除整个表
