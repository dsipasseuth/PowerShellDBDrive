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

        private const string SELECT_SCHEMAS_NAMES = "SELECT schema_name FROM information_schema.schemata";

        private const string SELECT_SCHEMAS_NAMES_REGEXP = "SELECT schema_name FROM information_schema.schemata where schema_name ~* @schemaname";

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

        private const string SELECT_TABLES_NAMES = "SELECT table_name FROM information_schema.tables where table_schema = @schemaname";

        private const string SELECT_TABLES_NAMES_REGEXP = "SELECT table_name FROM information_schema.tables where table_schema = @schemaname and table_name ~* @regexp";

        private const string SELECT_TABLE_EXISTS = "SELECT 1 FROM information_schema.tables WHERE table_schema = @schemaname AND table_name = @tablename";

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
	collation_catalog        ,
	collation_schema         ,
	collation_name           ,
	domain_catalog           ,
	domain_schema            ,
	domain_name              ,
	udt_catalog              ,
	udt_schema               ,
	udt_name                 ,
	maximum_cardinality      ,
	dtd_identifier           ,
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
                            yield return new PgDatabaseSchemaInfo(reader["CATALOG_NAME"] as string, reader["SCHEMA_OWNER"] as string, reader["SCHEMA_NAME"] as string);
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
                            return new PgDatabaseSchemaInfo(reader["CATALOG_NAME"] as string, reader["SCHEMA_OWNER"] as string, reader["SCHEMA_NAME"] as string);
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
                    command.CommandText = SELECT_SCHEMAS_NAMES;
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
                            yield return reader["SCHEMA_NAME"] as string;
                        }
                    }
                }
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
                BaseQueryManager bqm = new BaseQueryManager(connection);
                string query = DatabaseUtils.GetSelectStringForTable(schemaName, tableName);
                long count = 1;
                foreach (PSObject p in bqm.QueryForObjects(query, new Dictionary<string, object>() {}, Timeout)) {
                    yield return p;
                    if (count >= maxResult) {
                        yield break;
                    }
                    count++;
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
                    command.CommandText = SELECT_TABLE_EXISTS;
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
            dci.TableCatalog    = reader["table_catalog"] as string;
            dci.TableSchema     = reader["table_schema"] as string;
            dci.TableName       = reader["table_name"] as string;
            dci.ColumnName      = reader["column_name"] as string;
            dci.OrdinalPosition = reader["ordinal_position"] as int? ?? default(int);
            dci.ColumnDefault   = reader["column_default"] as string;
            dci.IsNullable      = reader["is_nullable"] as bool? ?? (default(bool));
            dci.DataType        = reader["data_type"] as string;
            dci.CharacterMaximumLength  = reader["character_maximum_length"] as int?;
            dci.CharacterOctetLength    = reader["character_octet_length"] as int?;
            dci.NumericPrecision    = reader["numeric_precision"] as int?;
            dci.NumericPrecisionRadix   = reader["numeric_precision_radix"] as int?;
            dci.NumericScale    =  reader["numeric_scale"] as int?;
            dci.DatetimePrecision   =  reader["datetime_precision"] as int?;
            dci.IntervalType    =  reader["interval_type"] as string;
            dci.CollationCatalog    =  reader["collation_catalog"] as string;
            dci.CollationSchema =  reader["collation_schema"] as string;
            dci.CollationName   =  reader["collation_name"] as string;
            dci.DomainCatalog   =  reader["domain_catalog"] as string;
            dci.DomainSchema    =  reader["domain_schema"] as string;
            dci.DomainName  =  reader["domain_name"] as string;
            dci.UdtCatalog  =  reader["udt_catalog"] as string;
            dci.UdtSchema   =  reader["udt_schema"] as string;
            dci.UdtName = reader["udt_name"] as string;
            dci.MaximumCardinality = reader["maximum_cardinality"] as int?;
            dci.DtdIdentifier = reader["dtd_identifier"] as string;
            dci.IsUpdatable = reader["is_updatable"] as bool? ?? default(bool);
            return dci;
        }

        #endregion Utility Methods
    }
}