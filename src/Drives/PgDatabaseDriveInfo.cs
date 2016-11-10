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

		private const string SELECT_DATABASES = "SELECT datname FROM pg_database WHERE datistemplate = false";

        private const string SELECT_SCHEMAS = "SELECT catalog_name, schema_owner, schema_name FROM information_schema.schemata";

        private const string SELECT_SCHEMA = "SELECT catalog_name, schema_owner, schema_name FROM information_schema.schemata where schema_name = @schemaname";

        private const string SELECT_SCHEMA_EXISTS = "SELECT 1 FROM information_schema.schemata WHERE schema_name = @schemaname";
		
        private const string SELECT_TABLES =
@"SELECT 
	table_catalog,
	table_schema,
	table_name, 
	table_type, 
	self_referencing_column_name, 
	reference_generation, 
	user_defined_type_catalog, 
	user_defined_type_schema, 
	user_defined_type_name,
	is_insertable_into,
	is_typed,
	commit_action
FROM information_schema.tables where table_schema = @schemaname";

        private const string SELECT_COLUMNS =
@"SELECT 
	table_catalog            ,
	table_schema             ,
	table_name               ,
	column_name              ,
	ordinal_position         ,
	column_default           ,
	is_nullable              ,
	data_type                ,
	character_maximum_length ,
	character_octet_length   ,
	numeric_precision        ,
	numeric_precision_radix  ,
	numeric_scale            ,
	datetime_precision       ,
	interval_type            ,
	interval_precision       ,
	character_set_catalog    ,
	character_set_schema     ,
	character_set_name       ,
	collation_catalog        ,
	collation_schema         ,
	collation_name           ,
	domain_catalog           ,
	domain_schema            ,
	domain_name              ,
	udt_catalog              ,
	udt_schema               ,
	udt_name                 ,
	scope_catalog            ,
	scope_schema             ,
	scope_name               ,
	maximum_cardinality      ,
	dtd_identifier           ,
	is_self_referencing      ,
	is_identity	yes_or_no    ,
	identity_generation      ,
	identity_start           ,
	identity_increment       ,
	identity_maximum         ,
	identity_minimum         ,
	identity_cycle           ,
	is_generated             ,
	generation_expression    ,
	is_updatable             
FROM information_schema.columns WHERE table_schema = @schemaname AND table_name = @tablename";

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
                            yield return new PgDatabaseSchemaInfo(reader.GetString(reader.GetOrdinal("CATALOG_NAME")), reader.GetString(reader.GetOrdinal("SCHEMA_OWNER")), reader.GetString(reader.GetOrdinal("SCHEMA_NAME")));
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
                            return new PgDatabaseSchemaInfo(reader.GetString(reader.GetOrdinal("CATALOG_NAME")), reader.GetString(reader.GetOrdinal("SCHEMA_OWNER")), reader.GetString(reader.GetOrdinal("SCHEMA_NAME")));
                        }
                    }
                }
                return null;
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
                            yield return reader["SCHEMA_NAME"] as string;
                        }
                    }
                }
            }
        }
		
        public override IEnumerable<String> GetSchemasNames(string regexp)
        {
            throw new NotImplementedException();
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
		
        public override IEnumerable<String> GetTablesNames(string schemaName) {
			return null;
		}
		
		public override IEnumerable<String> GetTablesNames(string schemaName, string tableName) {
			return null;
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
            throw new NotImplementedException();
        }
        
        #region Utility Methods 


        private PgDatabaseTableInfo BuildDatabaseTableInfo(DbDataReader reader)
        {
            PgDatabaseTableInfo dti = new PgDatabaseTableInfo();
            dti.TableCatalog = reader["table_catalog"] as string;
            dti.TableSchema = reader["table_schema"] as string;
            dti.TableName = reader["table_name"] as string; 
            dti.TableType = reader["table_type"] as string;
            dti.SelfReferencingColumnName = reader["self_referencing_column_name"] as string;
            dti.ReferenceGeneration = reader["reference_generation"] as string;
            dti.UserDefinedTypeCatalog = reader["user_defined_type_catalog"] as string; 
            dti.UserDefinedTypeSchema = reader["user_defined_type_schema"] as string; 
            dti.UserDefinedTypeName = reader["user_defined_type_name"] as string;
            dti.IsInsertableInto = reader["is_insertable_into"] as bool?;
            dti.IsTyped = reader["is_typed"] as bool?;
            dti.CommitAction = reader["commit_action"] as string;
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