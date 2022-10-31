### wea-recover用户手册

#### 介绍

通过binlog起始文件名+起止时间段，导出指定表在某时间段被修改的前镜像，导出SQL文件为DBA提供数据恢复依据。

#### 示例

##### 示例1

通过MySQL实例3306,恢复testdb.test1表,并将恢复数据导出到./test1_recover.sql文件中,原始SQL保存在./raw.sql中
过滤条件为:

- 开始位点: binlog.000040:0
- 结束位点: binlog.000042
- 开始时间: 2022-01-02_15:04:05
- 结束时间: 2023-10-02_15:04:05

```shell script
./wea-recover -h 127.0.0.1 -P 3306 -u root -p 123456 -D testdb -t test1 --start-datetime 2022-01-02_15:04:05 --stop-datetime 2023-10-02_15:04:05 --start-position binlog.000040:0 --stop-position binlog.000042 --export
```

##### 示例2

通过目录/binlog/path下的binlog,恢复testdb.test1表,并将恢复数据导出到./test1_recover.sql文件中,原始SQL保存在./raw.sql中
过滤条件为:

- 开始位点: binlog.000040:0
- 结束位点: binlog.000042:65535
- 开始时间: 2022-01-02_15:04:05
- 结束时间: 2023-10-02_15:04:05

```shell script
./wea-recover --addr 127.0.0.1 --port 3306 --user root --pwd 123456 --db testdb --table test1 --binlog-path "/binlog/path" --start-datetime 2022-01-02_15:04:05 --stop-datetime 2023-10-02_15:04:05 --start-position binlog.000040:0 --stop-position binlog.000042:65535 --export true
```

#### 参数详解

- --help : 使用帮助
- --addr/-h : 数据库IP
- --port/-P : 数据库端口
- --user/-u : 用户名
- --pwd/-p : 密码
- --db/-D : 待恢复的数据库
- --table/-t : 待恢复的表
- --binlog-path : binlog集合目录,不填则从MySQL实例中恢复数据
- --start-position : 恢复起始位点,eg:mysql-bin.001:44
- --stop-position : 恢复截止位点,eg:mysql-bin.010
- --start-datetime : 恢复起始时间,eg:2006-01-02_15:04:05
- --stop-datetime : 恢复截止时间,eg:2006-01-02_15:04:05
- --export : 是否导出数据到文件table_recover.sql
- --event-filter: 事件类型过滤,支持:update,delete,both(默认)