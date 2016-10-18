using System;
using System.Linq;
using System.Data;
using System.Management.Automation;
using System.Data.Common;
using System.Collections.Generic;
using PowerShellDBDrive.DataModel.PostgreSQL;
using PowerShellDBDrive.DataModel;

namespace PowerShellDBDrive.Drives
{
    /// <summary>
    /// PostgreSQL Database Implemention for DatabaseDriveInfo
    /// </summary>
    public class PgDatabaseDriveInfo : DatabaseDriveInfo
    {
        #region SQL Queries
        private const string SELECT_SCHEMAS = "SELECT USER_ID, USERNAME, CREATED FROM ALL_USERS";

        private const string SELECT_TABLES =
@"SELECT 
	OWNER ,
	TABLE_NAME ,
	TABLESPACE_NAME ,
	CLUSTER_NAME ,
	IOT_NAME ,
	STATUS ,
	PCT_FREE ,
	PCT_USED ,
	INI_TRANS ,
	MAX_TRANS ,
	INITIAL_EXTENT ,
	NEXT_EXTENT ,
	MIN_EXTENTS ,
	MAX_EXTENTS ,
	PCT_INCREASE ,
	FREELISTS ,
	FREELIST_GROUPS ,
	LOGGING ,
	BACKED_UP ,
	NUM_ROWS ,
	BLOCKS ,
	EMPTY_BLOCKS ,
	AVG_SPACE ,
	CHAIN_CNT ,
	AVG_ROW_LEN ,
	AVG_SPACE_FREELIST_BLOCKS ,
	NUM_FREELIST_BLOCKS ,
	DEGREE ,
	INSTANCES ,
	CACHE ,
	TABLE_LOCK ,
	SAMPLE_SIZE ,
	LAST_ANALYZED ,
	PARTITIONED ,
	IOT_TYPE ,
	TEMPORARY ,
	SECONDARY ,
	NESTED ,
	BUFFER_POOL ,
	FLASH_CACHE ,
	CELL_FLASH_CACHE ,
	ROW_MOVEMENT ,
	GLOBAL_STATS ,
	USER_STATS ,
	DURATION ,
	SKIP_CORRUPT ,
	MONITORING ,
	CLUSTER_OWNER ,
	DEPENDENCIES ,
	COMPRESSION ,
	COMPRESS_FOR ,
	DROPPED ,
	READ_ONLY ,
	SEGMENT_CREATED ,
	RESULT_CACHE
FROM ALL_TABLES WHERE OWNER = :schemaname ";

        private const string SELECT_COLUMNS =
@"SELECT Owner ,
  Table_Name ,
  Column_Name ,
  Data_Type ,
  Data_Type_Mod ,
  Data_Type_Owner ,
  Data_Length ,
  Data_Precision ,
  Data_Scale ,
  Nullable ,
  Column_Id ,
  Default_Length ,
  Data_Default ,
  Num_Distinct ,
  Low_Value ,
  High_Value ,
  Density ,
  Num_Nulls ,
  Num_Buckets ,
  Last_Analyzed ,
  Sample_Size ,
  Character_Set_Name ,
  Char_Col_Decl_Length ,
  Global_Stats ,
  User_Stats ,
  Avg_Col_Len ,
  Char_Length ,
  Char_Used ,
  V80_Fmt_Image ,
  Data_Upgraded ,
  Histogram
FROM ALL_TAB_COLUMNS WHERE OWNER = :schemaname AND TABLE_NAME = :tablename";

        #endregion SQL Queries

        public PgDatabaseDriveInfo(PSDriveInfo driveInfo, DatabaseParameters parameters) : base(driveInfo, parameters)
        {

        }

        public override IEnumerable<IDatabaseSchemaInfo> GetSchemas()
        {
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_SCHEMAS;
                    command.CommandTimeout = Timeout;
                    command.CommandType = CommandType.Text;
                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return new PgDatabaseSchemaInfo();
                        }
                    }
                }
            }
        }

        public override IEnumerable<String> GetSchemasNames()
        {
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_SCHEMAS;
                    command.CommandTimeout = Timeout;
                    command.CommandType = CommandType.Text;
                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return reader["USERNAME"] as string;
                        }
                    }
                }
            }
        }

        public override IDatabaseSchemaInfo GetSchema(string schemaName)
        {
            return (from s in GetSchemas() where string.Equals(s.SchemaName, schemaName, StringComparison.CurrentCultureIgnoreCase) select s).FirstOrDefault();
        }

        public override IEnumerable<IDatabaseTableInfo> GetTables(string schemaName)
        {
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_TABLES;
                    command.CommandTimeout = Timeout;
                    command.CommandType = CommandType.Text;
                    DbParameter parameter = command.CreateParameter();
                    parameter.DbType = DbType.String;
                    parameter.ParameterName = "schemaname";
                    parameter.Value = schemaName;
                    command.Parameters.Add(parameter);
                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            PgDatabaseTableInfo dti = BuildDatabaseTableInfo(reader);
                            dti.Columns = GetDatabaseColumnsInfo(schemaName, dti.TableName).ToArray();
                            yield return dti;
                        }
                    }
                }
            }
        }

        private IEnumerable<IDatabaseColumnInfo> GetDatabaseColumnsInfo(string schemaName, string tableName)
        {
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_COLUMNS;
                    command.CommandTimeout = Timeout;
                    command.CommandType = CommandType.Text;
                    DbParameter parameter = command.CreateParameter();
                    parameter.DbType = DbType.String;
                    parameter.ParameterName = "schemaname";
                    parameter.Value = schemaName;
                    command.Parameters.Add(parameter);
                    parameter = command.CreateParameter();
                    parameter.DbType = DbType.String;
                    parameter.ParameterName = "tablename";
                    parameter.Value = tableName;
                    command.Parameters.Add(parameter);
                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return BuildDatabaseColumnInfo(reader);
                        }
                    }
                }
            }
        }

        public override IDatabaseTableInfo GetTable(string schemaName, string tableName)
        {
            var tableList = GetTables(schemaName);
            return (from table in tableList where tableName.Equals(tableName) select table).FirstOrDefault();
        }

        public override IEnumerable<PSObject> GetRows(string schemaName, string tableName, int maxResult)
        {
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = DatabaseUtils.GetSelectStringForTable(tableName);
                    command.CommandTimeout = Timeout;
                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        PSObjectBuilder builder = new PSObjectBuilder();
                        while (reader.Read())
                        {
                            if (maxResult > 0)
                            {
                                builder.NewInstance();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    builder.AddField(reader.GetName(i), reader.GetValue(i), reader.GetFieldType(i));
                                }
                                yield return builder.Build();
                            }
                            else
                            {
                                yield break;
                            }
                            maxResult--;
                        }
                    }
                }
            }
        }

        #region Utility Methods 


        private PgDatabaseTableInfo BuildDatabaseTableInfo(DbDataReader reader)
        {
            PgDatabaseTableInfo dti = new PgDatabaseTableInfo();
            /*dti.Owner = reader["OWNER"] as string;
            dti.TableName = reader["TABLE_NAME"] as string;
            dti.TablespaceName = reader["TABLESPACE_NAME"] as string;
            dti.ClusterName = reader["CLUSTER_NAME"] as string;
            dti.IotName = reader["IOT_NAME"] as string;
            dti.Status = reader["STATUS"] as string;
            dti.PctFree = reader["PCT_FREE"] as long?;
            dti.PctUsed = reader["PCT_USED"] as long?;
            dti.IniTrans = reader["INI_TRANS"] as long?;
            dti.MaxTrans = reader["MAX_TRANS"] as long?;
            dti.InitialExtent = reader["INITIAL_EXTENT"] as long?;
            dti.NextExtent = reader["NEXT_EXTENT"] as long?;
            dti.MinExtents = reader["MIN_EXTENTS"] as long?;
            dti.MaxExtents = reader["MAX_EXTENTS"] as long?;
            dti.PctIncrease = reader["PCT_INCREASE"] as long?;
            dti.Freelists = reader["FREELISTS"] as long?;
            dti.FreelistGroups = reader["FREELIST_GROUPS"] as long?;
            dti.Logging = reader["LOGGING"] as string;
            dti.BackedUp = reader["BACKED_UP"] as string;
            dti.NumRows = reader["NUM_ROWS"] as long?;
            dti.Blocks = reader["BLOCKS"] as long?;
            dti.EmptyBlocks = reader["EMPTY_BLOCKS"] as long?;
            dti.AvgSpace = reader["AVG_SPACE"] as long?;
            dti.ChainCnt = reader["CHAIN_CNT"] as long?;
            dti.AvgRowLen = reader["AVG_ROW_LEN"] as long?;
            dti.AvgSpaceFreelistBlocks = reader["AVG_SPACE_FREELIST_BLOCKS"] as long?;
            dti.NumFreelistBlocks = reader["NUM_FREELIST_BLOCKS"] as long?;
            dti.Degree = reader["DEGREE"] as string;
            dti.Instances = reader["INSTANCES"] as string;
            dti.Cache = reader["CACHE"] as string;
            dti.TableLock = reader["TABLE_LOCK"] as string;
            dti.SampleSize = reader["SAMPLE_SIZE"] as long?;
            dti.LastAnalyzed = reader["LAST_ANALYZED"] as DateTime?;
            dti.Partitioned = reader["PARTITIONED"] as string;
            dti.IotType = reader["IOT_TYPE"] as string;
            dti.Temporary = reader["TEMPORARY"] as string;
            dti.Secondary = reader["SECONDARY"] as string;
            dti.Nested = reader["NESTED"] as string;
            dti.BufferPool = reader["BUFFER_POOL"] as string;
            dti.FlashCache = reader["FLASH_CACHE"] as string;
            dti.CellFlashCache = reader["CELL_FLASH_CACHE"] as string;
            dti.RowMovement = reader["ROW_MOVEMENT"] as string;
            dti.GlobalStats = reader["GLOBAL_STATS"] as string;
            dti.UserStats = reader["USER_STATS"] as string;
            dti.Duration = reader["DURATION"] as string;
            dti.SkipCorrupt = reader["SKIP_CORRUPT"] as string;
            dti.Monitoring = reader["MONITORING"] as string;
            dti.ClusterOwner = reader["CLUSTER_OWNER"] as string;
            dti.Dependencies = reader["DEPENDENCIES"] as string;
            dti.Compression = reader["COMPRESSION"] as string;
            dti.CompressFor = reader["COMPRESS_FOR"] as string;
            dti.Dropped = reader["DROPPED"] as string;
            dti.ReadOnly = reader["READ_ONLY"] as string;
            dti.SegmentCreated = reader["SEGMENT_CREATED"] as string;
            dti.ResultCache = reader["RESULT_CACHE"] as string;*/
            return dti;
        }

        private PgDatabaseColumnInfo BuildDatabaseColumnInfo(DbDataReader reader)
        {
            PgDatabaseColumnInfo dci = new PgDatabaseColumnInfo();
            /*dci.Owner = reader["OWNER"] as string;
            dci.TableName = reader["TABLE_NAME"] as string;
            dci.ColumnName = reader["COLUMN_NAME"] as string;
            dci.DataType = reader["DATA_TYPE"] as string;
            dci.DataTypeMod = reader["DATA_TYPE_MOD"] as string;
            dci.DataTypeOwner = reader["DATA_TYPE_OWNER"] as string;
            dci.DataLength = reader["DATA_LENGTH"] as long?;
            dci.DataPrecision = reader["DATA_PRECISION"] as long?;
            dci.DataScale = reader["DATA_SCALE"] as long?;
            dci.Nullable = reader["NULLABLE"] as string;
            dci.ColumnId = reader["COLUMN_ID"] as long?;
            dci.DefaultLength = reader["DEFAULT_LENGTH"] as long?;
            dci.DataDefault = reader["DATA_DEFAULT"] as long?;
            dci.NumDistinct = reader["NUM_DISTINCT"] as long?;
            dci.LowValue = reader["LOW_VALUE"] as string;
            dci.HighValue = reader["HIGH_VALUE"] as string;
            dci.Density = reader["DENSITY"] as long?;
            dci.NumNulls = reader["NUM_NULLS"] as long?;
            dci.NumBuckets = reader["NUM_BUCKETS"] as long?;
            dci.LastAnalyzed = reader["LAST_ANALYZED"] as DateTime?;
            dci.SampleSize = reader["SAMPLE_SIZE"] as long?;
            dci.CharacterSetName = reader["CHARACTER_SET_NAME"] as string;
            dci.CharColDeclLength = reader["CHAR_COL_DECL_LENGTH"] as long?;
            dci.GlobalStats = reader["GLOBAL_STATS"] as string;
            dci.UserStats = reader["USER_STATS"] as string;
            dci.AvgColLen = reader["AVG_COL_LEN"] as long?;
            dci.CharLength = reader["CHAR_LENGTH"] as long?;
            dci.CharUsed = reader["CHAR_USED"] as string;
            dci.V80FmtImage = reader["V80_FMT_IMAGE"] as string;
            dci.DataUpgraded = reader["DATA_UPGRADED"] as string;
            dci.Histogram = reader["HISTOGRAM"] as string;*/
            return dci;
        }
        #endregion Utility Methods
    }
}