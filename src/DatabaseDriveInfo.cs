using System;
using System.Linq;
using System.Data;
using System.Management.Automation;
using System.Data.Common;
using System.Collections.Generic;

namespace PowerShellDBDrive
{
    public class DatabaseDriveInfo : PSDriveInfo
    {
		public const int DEFAULT_MAX_READ_RESULT = 100;
		
		public const int DEFAULT_BULK_READ_LIMIT = 50;
		
		public int MaxReadResult { get; set; }
		
		public int BulkReadLimit { get; set; }
		
		public string ParsedConnectionString { get; protected set; }
		
		private DatabaseParameters Parameters { get; set; }
		
		private DbProviderFactory Factory { get; set; }
		
        public DatabaseDriveInfo( PSDriveInfo driveInfo, DatabaseParameters parameters ) : base( driveInfo )
        {
			MaxReadResult = DEFAULT_MAX_READ_RESULT;
			BulkReadLimit = DEFAULT_BULK_READ_LIMIT;
			Factory = DbProviderFactories.GetFactory(parameters.Provider);
			DbConnectionStringBuilder csb = Factory.CreateConnectionStringBuilder();
			csb.ConnectionString = parameters.ConnectionString;
			ParsedConnectionString = csb.ConnectionString;
        }
		
		public DbConnection GetConnection() { 
			DbConnection connection = Factory.CreateConnection();
			connection.ConnectionString = ParsedConnectionString;
			return connection;
		}
		
		public IEnumerable<PSObject> GetRows(string schemaName, string tableName) {
			foreach (PSObject p in GetRows(schemaName, tableName, MaxReadResult)) {
				yield return p;
			}
		}
		
		/// <summary> 
		/// Return all schemas information. 
		/// </summary> 
		/// <returns>Collection of schema information objects.</returns> 
		public IEnumerable<DatabaseSchemaInfo> GetSchemas() {
			using (DbConnection connection = GetConnection()) {
				connection.Open();
				DataTable dt = connection.GetSchema("Users");
				foreach (DataRow dr in dt.Rows)
				{
					string schemaName = dr["NAME"] as string;
					DateTime? createdate = dr["CREATEDATE"] as DateTime?;
					yield return new DatabaseSchemaInfo(schemaName, createdate);
				}
			}
		}
		
		/// <summary> 
		/// Return given schema information.
		/// </summary> 
		/// <returns>Schema information objects.</returns>
		public DatabaseSchemaInfo GetSchema(string schemaName) {
			return (from s in GetSchemas() where string.Equals(s.Name, schemaName, StringComparison.CurrentCultureIgnoreCase) select s).FirstOrDefault();
		}
		
		/// TODO Rewrite
		/// <summary> 
		/// Retrieve the list of tables from the database. 
		/// </summary> 
		/// <returns> 
		/// Collection of DatabaseTableInfo objects, each object representing 
		/// information about one database table
		/// </returns> 
		public IEnumerable<DatabaseTableInfo> GetTables(string schemaName) 
		{
			using (DbConnection connection = GetConnection()) {
				connection.Open();
				string[] restriction = new string[2] { schemaName, null };
				DataTable tables = connection.GetSchema("Tables", restriction);
				using (DbCommand command = connection.CreateCommand())
				{
					foreach (DataRow table in tables.Rows)
					{
						string tableName = table["TABLE_NAME"] as string;
						DataTable columns = connection.GetSchema("Columns", new string[3] { schemaName, tableName, null });
						DatabaseColumnInfo[] dbColumns = new DatabaseColumnInfo[columns.Rows.Count];
						int i = 0;
						foreach (DataRow column in columns.Rows) {
							string columnName = column["TABLE_NAME"] as string;
							string dataType = column["DATATYPE"] as string;
							int? length = column["LENGTH"] as int?;
							int? precision = column["PRECISION"] as int?;
							int? scale = column["SCALE"] as int?;
							bool nullable = "N".Equals(column["NULLABLE"] as string);
							dbColumns[i++] = new DatabaseColumnInfo(schemaName, tableName, columnName, dataType, length, precision, scale, nullable);
						}
						string cmd = String.Format("Select count(1) as c from {0}.{1}", schemaName, tableName, dbColumns);
						command.CommandText = cmd;
						long count = Convert.ToInt64(command.ExecuteScalar());
						yield return new DatabaseTableInfo(schemaName, tableName, count, dbColumns);
					}
				}
			}
		}
		
		/// TODO Rewrite
		/// <summary> 
		/// Retrieve the list of tables from the database. 
		/// </summary> 
		/// <returns> 
		/// Collection of DatabaseTableInfo objects, each object representing 
		/// information about one database table
		/// </returns> 
		public DatabaseTableInfo GetTable(string schemaName, string tableName) 
		{
			var tableList = GetTables(schemaName);
			return (from table in tableList where tableName.Equals(tableName) select table).FirstOrDefault();
		}
		
		/// 
		public IEnumerable<PSObject> GetRows(string schemaName, string tableName, int maxResult) {
			using (DbConnection connection = GetConnection()) {
				connection.Open();
				DatabaseUtils.GetSelectStringForTable(tableName);
				using (DbCommand command = connection.CreateCommand())
				using (DbDataReader reader = command.ExecuteReader())
				{
					PSObjectBuilder builder = new PSObjectBuilder();
					while (reader.Read())
					{
						if (maxResult > 0)
						{
							builder.NewInstance();
							for (int i = 0; i < reader.FieldCount; i++) {
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
		
		/// TODO Rewrite to make select statement.
	    /// <summary> 
		/// Retrieves a single row from the named table. 
		/// </summary> 
		/// <param name="tableName">The table that contains the  
		/// numbered row.</param> 
		/// <param name="row">The index of the row to return.</param> 
		/// <returns>The specified table row.</returns> 
		public PSObject GetRow(string schemaName, string tableName, string row) 
		{
			// WriteError(new ErrorRecord(new ItemNotFoundException(), "RowNotFound", ErrorCategory.ObjectNotFound, row)); 
			return null;
		}
    }
}