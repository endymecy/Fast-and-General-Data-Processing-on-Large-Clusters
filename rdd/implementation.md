# 2.5　实现

# 2.5.1　任务调度

我们用大概34000行的`Scala`语言实现了`Spark`。系统运行在各种各样的集群`managers`（包括`Apache Mesos`【56】，`Hadoop YARN`【109】和`Amazon EC2`【4】）上，和它自己的内置集群`manager`。
每个`Spark`程序在集群上有着自己的驱动（`master`）和工作进程，作为一个独立的应用程序运行，并且这些应用程序间的资源共享是由集群`manager`处理的。

`Spark`能够借助`Hadoop`现有的输入插件`APIs`从任何一个`Hadoop`输入源（例如，`HDFS`或者`HBase`）读取数据，并且在`Scala`的一个未修改版本上运行。


![2.4](../images/2.4.png "2.4")

# 2.5.２　多租户

![2.5](../images/2.5.png "2.5")
