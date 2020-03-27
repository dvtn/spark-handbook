Spark中的专业术语

- Master(standalone): 资源管理的主节点（进程）
- Cluster Manager: 在集群上获取新源的外部服务（例如standalone, Mesos, Yarn)
- Worker Node(standalone):资源管理的从节点（进程）或者说管理本机资源的进程
- Application: 基于Spark的用户程序，包含了driver程序和运行在集群上的executor程序
- Driver Program: 用来连接工作进程(Worker)的程序
- Executor: 是在一个worker进程所管理的节点上为某个Application启动的一个进程，该进程负责运行任务，并且负责将数据存在内存或者磁盘上。每个应用程序都有各自独立的executors
- Task: 被送到某个executor上的工作单元
- Job: 包含很多任务(Task)的并行计算，可以看作和action对应
- Stage: 一个job会被拆分很多组任务，每组任务被称为Stage(就像MapReduce分map task和reduce task一样)

