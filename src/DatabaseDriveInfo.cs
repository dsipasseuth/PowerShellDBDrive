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
		
        public DatabaseDriveInfo( PSDriveInfo driveInfo, DatabaseParameters parameters ) : base( driveInfo )
        {
			MaxReadResult = DEFAULT_MAX_READ_RESULT;
			BulkReadLimit = DEFAULT_BULK_READ_LIMIT;
        }
		
		public DbConnection DatabaseConnection { get; set; }
		
		public IEnumerable<PSObject> GetRows(string tableName) {
			foreach (PSObject p in GetRows(tableName, MaxReadResult)) {
				yield return p;
			}
		}
		
		public IEnumerable<PSObject> GetRows(string tableName, int maxResult) {
			using (DbCommand command = di.DatabaseConnection.CreateCommand())
            {
                DatabaseUtils.GetSelectStringForTable(tableName);
                PSObjectBuilder builder = new PSObjectBuilder();
                using (DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        if (maxResult > 0)
                        {
                            builder.NewInstance();
                            builder.AddField("test", "coucou");
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
		
		/// <summary> 
		/// Return all schemas information. 
		/// </summary> 
		/// <returns>Collection of schema information objects.</returns> 
		public IEnumerable<PSObject> GetSchemas() {
			PSObject schemaBase = new PSObject();
			schemaBase.Members.Add(new PSNoteProperty("Name", "Test"));
			ICollection<PSObject> collection = new Collection<PSObject>();
			collection.Add(schemaBase);
			return collection;
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
			DatabaseDriveInfo di = this.PSDriveInfo as DatabaseDriveInfo;
			if (di == null) {
				yield break;
			}
			DbConnection connection = di.DatabaseConnection;
			DataTable dt = connection.GetSchema("Tables");

            // Iterate through all the rows in the schema and create DatabaseTableInfo 
            // objects which represents a table. 
            using (DbCommand command = (PSDriveInfo as DatabaseDriveInfo).DatabaseConnection.CreateCommand())
            {
                foreach (DataRow dr in dt.Rows)
                {
                    string tableName = dr["TABLE_NAME"] as string;
                    DataColumnCollection columns = null;
                    string cmd = String.Format("Select count(1) from {0}", tableName);
                    command.CommandText = cmd;
                    yield return new DatabaseTableInfo(dr, tableName, (int)command.ExecuteScalar() , columns);
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
		private DatabaseRowInfo GetRow(string tableName, string row) 
		{ 
			WriteError(new ErrorRecord(new ItemNotFoundException(), "RowNotFound", ErrorCategory.ObjectNotFound, row)); 
			return null; 
		}
    }
}