# mysqldump
[![Go Report Card](https://goreportcard.com/badge/github.com/Jrohy/mysqldump)](https://goreportcard.com/report/github.com/Jrohy/mysqldump)
[![Downloads](https://img.shields.io/github/downloads/Jrohy/mysqldump/total.svg)](https://img.shields.io/github/downloads/Jrohy/mysqldump/total.svg)

go语言版mysqldump, goroutine并发导sql, 比navicat等工具导出sql快！

支持 **mysql5.x版本**

此项目为: https://github.com/xelabs/go-mydumper 项目的修改优化版

如下变更:
- 修复视图view导出后无法导入的问题
- 增加function的导入导出
- 加入mysql source组合源命令(-m), 传参 -m user:pass@host:port, 简化传参
- 加入排除指定table数据导出sql 命令(-exclude)
- 优化日志格式和运行时间的显示
- 合并导入和导出sql功能到同一个文件(-i/-o来分区)
- 提供所有平台的release编译文件

## 命令行
```
./mysqldump -h [HOST] -P [PORT] -u [USER] -p [PASSWORD] -db [DATABASE] -o [OUTDIR] -i [INDIR] -m [MYSQL_SOURCE] -exclude [EXCLUDE_TABLE]
    -h        string    数据库连接地址
    -P        int       数据库连接端口(不传则默认3306)
    -u        string    连接用户名
    -p        string    连接密码
    -m        string    数据库连接信息, 格式 user:pass@host:port(此命令用来简化连接数据库传参信息)
    -db       string    指定的数据库名, 导出sql模式必要, 导入sql模式可选,导入模式时为指定要导入的数据库名(不一定和原来导出的数据库名一致)
    -o        string    导出数据库到指定的目录路径, 此命令存在则指定为导出sql模式
    -i        string    指定要导入的sql所在目录路径, 此命令存在则指定为导入sql模式
    -exclude  string    指定要排除的table数据(只导表结构),多个排除的表用英文','隔开
    -d                  导入sql模式时存在相同名字表格时是否先删除(覆盖), 存在此命令则表示先删除同名表格
    -t        int       指定线程数(默认16)
    -table    string    导出特定的表
    -s        int       insert语句的大小(单位byte, 默认1000000)
```