---
displayed_sidebar: docs
keywords: ['shitu']
---

# CREATE VIEW

## 功能

创建一个视图。

视图（或逻辑视图）是一种虚拟表，其中的数据来自于对其他现有实体表的查询结果。因此，视图无需占用物理存储空间。所有针对视图的查询相当于该视图对应查询语句之上的子查询。

关于 StarRocks 支持的物化视图，请参阅[同步物化视图](../../../using_starrocks/Materialized_view-single_table.md)和[异步物化视图](../../../using_starrocks/async_mv/Materialized_view.md)。

从 v3.4.1 开始，StarRocks 支持安全视图。您可以禁止没有基表 SELECT 权限的用户查询视图。

> **注意**
>
> 该操作需要有指定数据库的 CREATE VIEW 权限。

## 语法

```SQL
CREATE [OR REPLACE] VIEW [IF NOT EXISTS]
[<database>.]<view_name>
(
    <column_name>[ COMMENT 'column comment']
    [, <column_name>[ COMMENT 'column comment'], ...]
)
[COMMENT 'view comment']
[SECURITY {NONE | INVOKER}]
AS <query_statement>
```

## 参数说明

| **参数**        | **说明**                                                      |
| --------------- | ------------------------------------------------------------ |
| OR REPLACE      | 替换已有视图。                                                 |
| database        | 视图所属的数据库名。                                            |
| view_name       | 视图名。命名要求参见[系统限制](../../System_limit.md)。           |
| column_name     | 视图中的列名。请注意，视图中的列和 `query_statement` 中查询的列的数量必须一致。 |
| COMMENT         | 视图中的列或视图本身的注释。                                 |
| SECURITY        | 指定在视图调用的访问权限检查时使用的安全上下文。有效值：<ul><li>`NONE`（默认）：拥有视图 SELECT 权限的用户即可查询视图。</li><li>`INVOKER`：仅同时拥有视图和其所引用的基表的 SELECT 权限的用户可查询视图。</li></ul> |
| query_statement | 用于创建视图的查询语句。可以为 StarRocks 支持的任意查询语句。 |

## 使用说明

- 查询视图需要该视图的 SELECT 权限和其对应基表的 SELECT 权限。
- 如果基表变更导致创建视图的查询语句无法执行，则查询该视图时报错。

## 示例

示例一：通过基于表 `example_table` 的聚合查询在数据库 `example_db` 中创建名为 `example_view` 的视图。

```SQL
CREATE VIEW example_db.example_view (k1, k2, k3, v1)
AS
SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
WHERE k1 = 20160112 GROUP BY k1,k2,k3;
```

示例二：通过基于表 `example_table` 的聚合查询在数据库 `example_db` 中创建名为 `example_view` 的视图，并为视图和其中的列设置注释。

```SQL
CREATE VIEW example_db.example_view
(
    k1 COMMENT 'first key',
    k2 COMMENT 'second key',
    k3 COMMENT 'third key',
    v1 COMMENT 'first value'
)
COMMENT 'my first view'
AS
SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
WHERE k1 = 20160112 GROUP BY k1,k2,k3;
```

示例三：通过基于表 `example_table` 的查询在数据库 `example_db` 中创建名为 `example_view` 的安全视图。该视图仅允许拥有基表 `example_table` 的 SELECT 权限的用户查询。

```SQL
CREATE VIEW example_db.example_view (k1, k2, k3, v1)
COMMENT 'my secure view'
SECURITY INVOKER
AS
SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
WHERE k1 = 20160112 GROUP BY k1,k2,k3;
```

## 相关 SQL

- [SHOW CREATE VIEW](SHOW_CREATE_VIEW.md)
- [ALTER VIEW](ALTER_VIEW.md)
- [DROP VIEW](DROP_VIEW.md)
