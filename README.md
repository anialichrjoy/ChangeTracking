# Abstract 
Solution to detect the incremental data updates on SQL Server Change tracked table(s) using t-SQL and SSIS. This pattern can be used to stage the incremental data for subsequent ETL by joining to a centralized reference table for easy selection and custom filtering.

## Technologies 
Microsoft SQL Server native **Change Tracking, t-SQL** and **SSIS**


## DDL

### Enable Change Tracking at Database level:

```sql 
ALTER DATABASE [DatabaseName] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 3 DAYS, AUTO_CLEANUP = ON);
```

### Enable Change Tracking at Table level:

```sql 
ALTER TABLE <EPIC TABLE NAME> ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = OFF);`
```

### Create Custom Objects: 

#### CREATE SCHEMA
A schema to maintain all custom CT objects

```sql 
CREATE SCHEMA [CT] AUTHORIZATION [dbo]
```

#### CT.Version

To track CT version (incremental between last processed and cutover) by table, a new table to CT schema. Initial version starts with the minimum valid version as maintained by the MSSQL database engine.

```sql 
CREATE TABLE [CT].[Version]
(
    [TableVersionID] [BIGINT] IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    [TableName] [NVARCHAR](128) NOT NULL,
    [VersionID] [BIGINT] NOT NULL,
    [CreatedOn] [DATETIME] NOT NULL,
    [CreatedBy] [NVARCHAR](128) NOT NULL,
    [UpdatedOn] [DATETIME] NULL,
    [UpdatedBy] [NVARCHAR](128) NULL
) GO

ALTER TABLE [CT].[Version]
ADD CONSTRAINT [Version_DK_CreatedOn] DEFAULT (GETDATE()) FOR [CreatedOn];
GO

ALTER TABLE [CT].[Version]
ADD CONSTRAINT [Version_DK_CreatedBy] DEFAULT (SUSER_SNAME()) FOR [CreatedBy];
GO
```

#### CT.Incremental

To centralize all Incremental changes for the Change Tracked tables, a table in CT schema. This table will store the net incremental changes until truncated and reloaded as part of the nightly ETL job stream.

```sql 
CREATE TABLE [CT].[Incremental]
(
[IncrementalID] [bigint] IDENTITY(1,1) NOT NULL PRIMARY KEY,
[Schema] [nvarchar](128) NOT NULL,
[Table] [nvarchar](128) NOT NULL,
[KeyName] [nvarchar](256) NOT NULL,
[KeyChecksumValue] [int] NOT NULL,
[CreatedOn] [datetime] NOT NULL,
[CreatedBy] [nvarchar](128) NOT NULL
) GO

ALTER TABLE [CT].[Incremental] ADD  CONSTRAINT [Incremental_DF_CreatedOn]  DEFAULT (getdate()) FOR [CreatedOn]
GO

ALTER TABLE [CT].[Incremental] ADD  CONSTRAINT [Incremental_DF_CreatedBy]  DEFAULT (suser_sname()) FOR [CreatedBy]
GO
```

# SSIS Workflow

Integration Service Package to orchestrate the refresh of  centralized Incremental table. High-level scope of the SSIS package and its data flow:

## Seed CT.Version (if not in the list) 
With minimum valid version (maintained by the MSSQL database engine) for tables that have been flagged for Change Tracking. 

```sql 
WITH CTE_ChangeTrackedTables AS (SELECT CONCAT(QUOTENAME(s.name), '.', QUOTENAME(t.name)) AS [SchemaTable],
                                        tr.[min_valid_version] AS [MinValidVersion]
                                 FROM sys.schemas s
                                     INNER JOIN sys.tables t ON s.schema_id = t.schema_id
                                     INNER JOIN sys.change_tracking_tables tr ON t.object_id = tr.object_id)
MERGE [EPIC_US_CT].[CT].[Version] AS t
USING
(
    SELECT [SchemaTable],
           [MinValidVersion]
    FROM CTE_ChangeTrackedTables
) AS s
([Schematable], [MinValidVersion])
ON t.[TableName] = s.[Schematable]
WHEN NOT MATCHED BY TARGET THEN
    INSERT
    (
        [TableName],
        [VersionID]
    )
    VALUES
    ([Schematable], [MinValidVersion]);
```

## Establish Cutover Change Tracking Version
```sql 
SELECT ? = CHANGE_TRACKING_CURRENT_VERSION()
```
## Initialize Incremental
```sql 
TRUNCATE TABLE [CT].[Incremental];
```

## Enumerate Change Tracked Tables (to refresh)

For each table, dynamically generate t-SQL to refresh the centralized Incremental table between last and established cutover Tracking version. Single column primary key and composite columns primary keys are supported and all references to the metadata are on-the-fly resolve:

```sql 
WITH CTE_list AS (SELECT QUOTENAME(s.name) AS [Schema],
                         QUOTENAME(t.name) AS [Table],
                         QUOTENAME(tc.[name]) AS [Column],
                         ic.key_ordinal AS [KeyOrdinal],
                         CONCAT(QUOTENAME(s.name), '.', QUOTENAME(t.name)) AS [Schematable],
                         VersionID =
                         (
                             SELECT ISNULL([VersionID], 0) AS [VersionID]
                             FROM [CT].[Version]
                             WHERE [TableName] = CONCAT(QUOTENAME(s.name), '.', QUOTENAME(t.name))
                         )
                  FROM sys.schemas s
                      INNER JOIN sys.tables t ON s.schema_id = t.schema_id
                      INNER JOIN sys.indexes i ON t.object_id = i.object_id
                      INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                      INNER JOIN sys.columns tc ON ic.object_id = tc.object_id AND ic.column_id = tc.column_id
                      INNER JOIN sys.change_tracking_tables tr ON t.object_id = tr.object_id
                  WHERE i.[is_primary_key] = 1),
     CTE_sub AS (SELECT [Schema],
                        [Table],
                        [Schematable],
                        [VersionID],
                        STUFF(
                        (
                            SELECT ',' + [Column]
                            FROM CTE_list sl
                            WHERE ml.[Table] = sl.[Table]
                            ORDER BY sl.[KeyOrdinal]
                            FOR XML PATH('')
                        ), 1, 1, ''
                             ) AS [Column]
                 FROM CTE_list ml
                 GROUP BY [Schema],
                          [Table],
                          [Schematable],
                          [VersionID])
SELECT [Schema],
       [Table],
       [Schematable],
       [Column],
       CONCAT(
                 'INSERT INTO [EPIC_CT_US].[CT].[Incremental] ([Schema], [Table], [KeyName], [KeyChecksumValue]) SELECT ', CHAR(39),
                 [Schema], CHAR(39), ' AS [Schema] , ', CHAR(39), [Table], CHAR(39), ' AS [Table] , ', CHAR(39),
                 [Column], CHAR(39), ' AS [Column], ', 'CHECKSUM(', [Column], ')',
                 ' AS [KeyValue] FROM CHANGETABLE(CHANGES ', [Schematable], ', ', [VersionID],
                 ') AS ct WHERE [CT].[SYS_CHANGE_VERSION] <= ?;'
             ) AS [CTQuery],
       [UpdateQuery] = CONCAT(
                                 'UPDATE [CT].[Version] SET [VersionID] = CAST(? AS BIGINT), [UpdatedOn] = GETDATE(), [UpdatedBy] = SUSER_SNAME() WHERE [TableName] = ', CHAR(39),
                                 [SchemaTable], CHAR(39)
                             )
FROM CTE_sub
ORDER BY [Schema],
         [Table];
```

## FOR EACH Enumerated

### Execute dynamically generated DMLs for Table in the current iteration 

For example:
```sql
INSERT INTO [EPIC_US_CT].[CT].[Incremental]
(
    [Schema],
    [Table],
    [KeyName],
    [KeyChecksumValue]
)
SELECT '[dbo]' AS [Schema],
       '[Product]' AS [Table],
       '[ProductID]' AS [Column],
       CHECKSUM([ProductID]) AS [KeyValue]
FROM CHANGETABLE(CHANGES [dbo].[Product], 594) AS ct
WHERE [ct].[SYS_CHANGE_VERSION] <= 815;
```

### Also establish its watermark version for the next run 

For example:
```sql 
UPDATE [CT].[Version]
SET [VersionID] = CAST(815 AS BIGINT),
    [UpdatedOn] = GETDATE(),
    [UpdatedBy] = SUSER_SNAME()
WHERE [TableName] = '[dbo].[Product]';
```

