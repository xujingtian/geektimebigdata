### 上传应用和数据

#### 创建目录

> mkdir -p xujingtian/week2

#### 上传程序和数据

windows环境下，cmd进入对应目录：

> scp HTTP_20130313143750.dat student5@114.55.52.33:~/xujingtian/week2
>
> scp mapreduce-1.0-xujingtian.jar student5@114.55.52.33:~/xujingtian/week2



### 初始化HDFS

#### 创建目录

> hadoop fs -mkdir /user/student5/xujingtian/week2/input 

![](D:\99.dean_pc\08.geektime\13.大数据\03.code\week2\mobileflow\README.assets\image-20220312182203893.png)



#### 将数据上传到HDFS

> hadoop fs -put /home/student5/xujingtian/week2/HTTP_20130313143750.dat  /user/student5/xujingtian/week2/input



![image-20220312182642970](D:\99.dean_pc\08.geektime\13.大数据\03.code\week2\mobileflow\README.assets\image-20220312182642970.png)





### 运行MapReduce

#### 运行jar程序

 参数1为输入input目录里的数据文件，参数2为输入到output目录，参数3表示只调用3个Reduce

> hadoop jar /home/student5/xujingtian/week2/mapreduce-1.0-xujingtian.jar  /user/student5/**xujingtian**/week2/input/HTTP_20130313143750.dat    /user/student5/xujingtian/week2/output 3



#### 运行完毕后显示结果 

> hadoop fs -ls /user/student5/xujingtian/week2/output 
>
> Found 4 items                                                                                                                                                                              
>
> -rw-r-----   2 student5 hadoop          0 2022-03-12 18:37 /user/student5/xujingtian/week2/output/_SUCCESS                                                                                 
>
> -rw-r-----   2 student5 hadoop        137 2022-03-12 18:37 /user/student5/xujingtian/week2/output/part-r-00000                                                                             
>
> -rw-r-----   2 student5 hadoop        228 2022-03-12 18:37 /user/student5/xujingtian/week2/output/part-r-00001                                                                             
>
> -rw-r-----   2 student5 hadoop        186 2022-03-12 18:37 /user/student5/xujingtian/week2/output/part-r-00002   
>
> hadoop fs -cat /user/student5/xujingtian/week2/output/part-r-00000
>
> 22/03/12 18:43:28 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false                                                       
>
> 13726230503     2481    24681   27162                                                                                                                                                      
>
> 13726238888     2481    24681   27162                                                                                                                                                      
>
> 13926435656     132     1512    1644                                                                                                                                                       
>
> 15920133257     3156    2936    6092                                                                                                                                                       
>
> 15989002119     1938    180     2118  

