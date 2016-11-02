using System;
using System.Linq;
using System.Data;
using System.Management.Automation;
using System.Data.Common;
using System.Collections.Generic;
using PowerShellDBDrive.DataModel.Oracle;
using PowerShellDBDrive.DataModel;

namespace PowerShellDBDrive.Drives
{
    /// <summary>
    /// Oracle Database Implemention for DatabaseDriveInfo
    /// </summary>
    public class OracleDatabaseDriveInfo : DatabaseDriveInfo
    {
        #region SQL Queries
        private const string SELECT_SCHEMAS = "SELECT USER_ID, USERNAME, CREATED FROM ALL_USERS";

        private const string SELECT_SCHEMA = "SELECT USER_ID, USERNAME, CREATED FROM ALL_USERS WHERE USERNAME = :schemaname";

        private const string SELECT_SCHEMA_EXISTS = "SELECT 1 FROM ALL_USERS WHERE USERNAME = :schemaname";

        private const string SELECT_SCHEMAS_NAMES = "SELECT USERNAME FROM ALL_USERS";
		
		private const string SELECT_SCHEMAS_NAMES_REGEXP = "SELECT USERNAME FROM ALL_USERS WHERE REGEXP_LIKE(USERNAME, :regexp)";

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
FROM ALL_TABLES WHERE OWNER = :schemaname";

private const string SELECT_SINGLE_TABLE =
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
FROM ALL_TABLES WHERE OWNER = :schemaname AND TABLE_NAME = :tablename ";

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

		private const string SELECT_TABLES_NAMES = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :schemaname ";
		
		private const string SELECT_TABLES_NAMES_REGEXP = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :schemaname AND REGEXP_LIKE(TABLE_NAME, :regexp)";

        private const string SELECT_TABLE_EXISTS = "SELECT 1 FROM ALL_TABLES WHERE OWNER = :schemaname AND REGEXP_LIKE(TABLE_NAME, :regexp)";

        #endregion SQL Queries

        public OracleDatabaseDriveInfo(PSDriveInfo driveInfo, DatabaseParameters parameters) : base(driveInfo, parameters)
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
                            yield return new OracleDatabaseSchemaInfo((long)reader.GetInt64(reader.GetOrdinal("USER_ID")), reader.GetString(reader.GetOrdinal("USERNAME")), reader.GetDateTime(reader.GetOrdinal("CREATED")));
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
                    command.CommandText = SELECT_SCHEMAS_NAMES;
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
		
        public override IEnumerable<String> GetSchemasNames(string regexp)
        {
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_SCHEMAS_NAMES_REGEXP;
                    command.CommandTimeout = Timeout;
                    command.CommandType = CommandType.Text;
					DbParameter parameter = command.CreateParameter();
                    parameter.DbType = DbType.String;
                    parameter.ParameterName = "regexp";
                    parameter.Value = "^" + regexp;
                    command.Parameters.Add(parameter);
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
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_SCHEMA;
                    command.CommandTimeout = Timeout;
                    command.CommandType = CommandType.Text;
                    DbParameter parameter = command.CreateParameter();
                    parameter.DbType = DbType.String;
                    parameter.ParameterName = "schemaname";
                    parameter.Value = schemaName;
                    command.Parameters.Add(parameter);
                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return new OracleDatabaseSchemaInfo((long)reader.GetInt64(reader.GetOrdinal("USER_ID")), reader.GetString(reader.GetOrdinal("USERNAME")), reader.GetDateTime(reader.GetOrdinal("CREATED")));
                        }
                    }
                }
                return null;
            }
        }
        
        public override IEnumerable<ObjectType> GetSupportedObjectTypes(string schemaName)
        {
            foreach (ObjectType ot in Enum.GetValues(typeof(ObjectType)))
            {
                yield return ot;
            }
        }

        public override IEnumerable<IDatabaseViewInfo> GetViews(string schemaName)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> GetViewsNames(string schemaName)
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<string> GetViewsNames(string schemaName, string viewName)
        {
            throw new NotImplementedException();
        }

        public override IDatabaseViewInfo GetView(string schemaName, string viewName)
        {
            throw new NotImplementedException();
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
                            OracleDatabaseTableInfo dti = BuildDatabaseTableInfo(reader);
                            dti.Columns = GetDatabaseColumnsInfo(schemaName, dti.TableName).ToArray();
                            yield return dti;
                        }
                    }
                }
            }
        }
		
        public override IEnumerable<String> GetTablesNames(string schemaName) {
			using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_TABLES_NAMES;
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
                            yield return reader["TABLE_NAME"] as string;
                        }
                    }
                }
            }
		}
		
		public override IEnumerable<String> GetTablesNames(string schemaName, string tableName) {
			using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_TABLES_NAMES_REGEXP;
                    command.CommandTimeout = Timeout;
                    command.CommandType = CommandType.Text;
                    DbParameter parameter = command.CreateParameter();
                    parameter.DbType = DbType.String;
                    parameter.ParameterName = "schemaname";
                    parameter.Value = schemaName;
                    command.Parameters.Add(parameter);
					parameter = command.CreateParameter();
                    parameter.DbType = DbType.String;
                    parameter.ParameterName = "regexp";
                    parameter.Value = schemaName;
                    command.Parameters.Add(parameter);
                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return reader["TABLE_NAME"] as string;
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
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_SINGLE_TABLE;
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
                        if (reader.Read())
                        {
                            OracleDatabaseTableInfo dti = BuildDatabaseTableInfo(reader);
                            dti.Columns = GetDatabaseColumnsInfo(schemaName, dti.TableName).ToArray();
                            return dti;
                        }
						return null;
                    }
                }
            }
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
        
        public override bool IsSchemaExist(string schemaName)
        {
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_SCHEMA_EXISTS;
                    command.CommandTimeout = Timeout;
                    command.CommandType = CommandType.Text;
                    DbParameter parameter = command.CreateParameter();
                    parameter.DbType = DbType.String;
                    parameter.ParameterName = "schemaname";
                    parameter.Value = schemaName;
                    command.Parameters.Add(parameter);
                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return true;
                        }
                    }
                }
                return false;
            }
        }

        public override bool IsObjectExist(string schemaName, ObjectType objectType, string[] objectPath)
        {
            if (objectType == ObjectType.TABLE) {
                return IsTableExist(schemaName, objectPath[0]);
            }
            return false;
        }

        private bool IsTableExist(string schemaName, string tableName)
        {
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_SINGLE_TABLE;
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
                        if (reader.Read())
                        {
                            return true;
                        }
                        return false;
                    }
                }
            }
        }

        #region Utility Methods 


        private OracleDatabaseTableInfo BuildDatabaseTableInfo(DbDataReader reader)
        {
            OracleDatabaseTableInfo dti = new OracleDatabaseTableInfo();
            dti.Owner = reader["OWNER"] as string;
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
            dti.ResultCache = reader["RESULT_CACHE"] as string;
            return dti;
        }

        private OracleDatabaseColumnInfo BuildDatabaseColumnInfo(DbDataReader reader)
        {
            OracleDatabaseColumnInfo dci = new OracleDatabaseColumnInfo();
            dci.Owner = reader["OWNER"] as string;
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
            dci.Histogram = reader["HISTOGRAM"] as string;
            return dci;
        }

        #endregion Utility Methods
    }
}