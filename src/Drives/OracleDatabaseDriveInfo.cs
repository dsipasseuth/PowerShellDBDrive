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

        #region Schema Queries

        private const string SELECT_SCHEMAS = "SELECT USER_ID, USERNAME, CREATED FROM ALL_USERS";

        private const string SELECT_SCHEMA = "SELECT USER_ID, USERNAME, CREATED FROM ALL_USERS WHERE USERNAME = :schemaname";

        private const string SELECT_SCHEMA_EXISTS = "SELECT 1 FROM ALL_USERS WHERE USERNAME = :schemaname";

        private const string SELECT_SCHEMAS_NAMES = "SELECT USERNAME FROM ALL_USERS";
		
		private const string SELECT_SCHEMAS_NAMES_REGEXP = "SELECT USERNAME FROM ALL_USERS WHERE REGEXP_LIKE(USERNAME, :regexp)";

        #endregion Schema Queries

        #region Table Queries

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

        private const string SELECT_TABLE_EXISTS = "SELECT 1 FROM ALL_TABLES WHERE OWNER = :schemaname AND TABLE_NAME = :tablename";

        #endregion Table Queries

        #region View Queries

        private const string SELECT_VIEWS = "SELECT OWNER, VIEW_NAME, TEXT_LENGTH, TEXT, TYPE_TEXT_LENGTH, TYPE_TEXT, OID_TEXT_LENGTH, OID_TEXT, VIEW_TYPE_OWNER, VIEW_TYPE, SUPERVIEW_NAME FROM ALL_VIEWS WHERE OWNER = :schemaname";

        private const string SELECT_VIEWS_NAME = "SELECT VIEW_NAME FROM ALL_VIEWS WHERE OWNER = :schemaname";

        private const string SELECT_VIEWS_NAMES_REGEXP = "SELECT VIEW_NAME FROM ALL_VIEWS WHERE OWNER = :schemaname AND REGEXP_LIKE(TABLE_NAME, :regexp)";

        private const string SELECT_VIEW_EXISTS = "SELECT 1 FROM ALL_VIEWS WHERE OWNER = :schemaname AND VIEW_NAME = :viewname";

        #endregion View Queries

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
                    parameter.Value = regexp;
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
            using (DbConnection connection = GetConnection())
            {
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = SELECT_VIEWS;
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
                            OracleDatabaseViewInfo dti = OracleDatabaseFactory.BuildDatabaseViewInfo(reader);
                            yield return dti;
                        }
                    }
                }
            }
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
                            OracleDatabaseTableInfo dti = OracleDatabaseFactory.BuildDatabaseTableInfo(reader);
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
                            yield return OracleDatabaseFactory.BuildDatabaseColumnInfo(reader);
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
                            OracleDatabaseTableInfo dti = OracleDatabaseFactory.BuildDatabaseTableInfo(reader);
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
    }
}