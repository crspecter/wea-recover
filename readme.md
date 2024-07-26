### wea-recover用户手册

#### 介绍

Wea-recover是一个MySQL数据库恢复工具，可以完成读取binlog文件中的update,delete语句，进行反向SQL生成。
1.配置访问数据库的账号需要replication slave,replication client，需要能创建数据库。
2.恢复的数据过程会在源端创建'test'数据库并创建数据恢复表
3.会在本地输出2份文件，一份是用于恢复的SQL文件，一份是通过binlog中的RowsQueryEvent输出的原始语句可以作为误操作依据
4.可以配置本地binlog解析模式，在源端MySQL无法操作时进行数据恢复
5.如果同一行在设置的恢复时间段被修改多次，会仅保留第一次修改的前镜像

通过binlog起始文件名+起止时间段，导出指定表在某时间段被修改的前镜像，导出SQL文件为DBA提供数据恢复依据。

#### 示例

##### 示例1

通过MySQL实例3306,恢复testdb.test1表,并将恢复数据导出到./test1_recover.sql文件中,原始SQL保存在./raw.sql中
过滤条件为:

- 开始位点: binlog.000040:0
- 结束位点: binlog.000042
- 开始时间: 2022-01-02 15:04:05
- 结束时间: 2023-10-02 15:04:05

```shell script
./wea-recover -h 127.0.0.1 -P 3306 -u root -D testdb -t test1 --start-datetime "2022-01-02 15:04:05" --stop-datetime "2023-10-02 15:04:05" --start-position binlog.000040:0 --stop-position binlog.000042 --export
```

##### 示例2

通过目录/binlog/path下的binlog,恢复testdb.test1表,并将恢复数据导出到./test1_recover.sql文件中,原始SQL保存在./raw.sql中
过滤条件为:

- 开始位点: binlog.000040:0
- 结束位点: binlog.000042:65535
- 开始时间: 2022-01-02 15:04:05
- 结束时间: 2023-10-02 15:04:05

```shell script
./wea-recover --addr 127.0.0.1 --port 3306 --user root --db testdb --table test1 --binlog-path "/binlog/path" --start-datetime "2022-01-02 15:04:05" --stop-datetime "2023-10-02 15:04:05" --start-position binlog.000040:0 --stop-position binlog.000042:65535 --export true
```

#### 参数详解

- --help : 使用帮助
- --addr/-h : 数据库IP
- --port/-P : 数据库端口
- --user/-u : 用户名
- --db/-D : 待恢复的数据库
- --table/-t : 待恢复的表
- --binlog-path : binlog集合目录,不填则从MySQL实例中恢复数据
- --start-position : 恢复起始位点,eg:mysql-bin.001:44
- --stop-position : 恢复截止位点,eg:mysql-bin.010
- --start-datetime : 恢复起始时间,eg:2006-01-02 15:04:05
- --stop-datetime : 恢复截止时间,eg:2006-01-02 15:04:05
- --export : 是否导出数据到文件table_recover.sql
- --event-filter: 事件类型过滤,支持:update,delete,both(默认)
