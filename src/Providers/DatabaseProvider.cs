using System;
using System.Linq;
using System.Collections.Generic;
using System.Management.Automation;
using System.Management.Automation.Provider;
using PowerShellDBDrive;
using System.Data.OleDb;

namespace PowerShellDBDrive.Provider
{
    [CmdletProvider( "DatabaseProvider", ProviderCapabilities.None )]
    public class DatabaseProvider : NavigationCmdletProvider 
    {
		#region Drive Manipulation 
		
		/// <summary> 
		/// The Windows PowerShell engine calls this method when the New-Drive  
		/// cmdlet is run. This provider creates a connection to the database  
		/// file and sets the Connection property in the PSDriveInfo. 
		/// </summary> 
		/// <param name="drive"> 
		/// Information describing the drive to create. 
		/// </param> 
		/// <returns>An object that describes the new drive.</returns> 
        protected override PSDriveInfo NewDrive( PSDriveInfo drive )
        {
			// Check to see if the supplied drive object is null. 
			if (drive == null) {
				WriteError(new ErrorRecord(
									   new ArgumentNullException("drive"),
									   "NullDrive",
									   ErrorCategory.InvalidArgument,
									   null));
				return null;
			}
			
			if (String.IsNullOrEmpty(drive.Root)) {
				WriteError(new ErrorRecord(
									   new ArgumentNullException("drive.Root"),
									   "NullRoot",
									   ErrorCategory.InvalidArgument,
									   null));
				return null;
			}
			var driveParams = this.DynamicParameters as DatabaseParameters;
			var driveInfo = new DatabaseDriveInfo(drive, driveParams);
			var connection = new OleDbConnection(driveParams.ConnectionString);
			connection.Open();
			driveInfo.DatabaseConnection = connection;
            return driveInfo;
        }
		
		/// <summary> 
		/// The Windows PowerShell engine calls this method when the  
		/// Remove-Drive cmdlet is run. 
		/// </summary> 
		/// <param name="drive">The drive to remove.</param> 
		/// <returns>The drive to be removed.</returns> 
		protected override PSDriveInfo RemoveDrive(PSDriveInfo drive) { 
			// Check to see if the supplied drive object is null. 
			if (drive == null) { 
				WriteError(new ErrorRecord( 
										new ArgumentNullException("drive"), 
										"NullDrive", 
										ErrorCategory.InvalidArgument, 
										drive));
				return null; 
			}
			
			var driveInfo = drive as DatabaseDriveInfo;
			if (driveInfo == null) {
				return null;
			}
			
			driveInfo.DatabaseConnection.Close();
			
			return driveInfo;
		}
		
        protected override object NewDriveDynamicParameters() {
            return new DatabaseParameters();
        }
		
		#endregion Drive Manipulation
		
		
		#region Item Methods
		
		/// <summary>
		/// Checks to see if a given path is actually a drive name. 
		/// </summary>
		/// <param name="path">The path to investigate.</param> 
		/// <returns>
		/// True if the path represents a drive; otherwise false is returned. 
		/// </returns>
		private bool PathIsDrive(string path) {
			// Remove the drive name and first path separator.  If the
			// path is reduced to nothing, it is a drive. Also if it is
			// just a drive then there will not be any path separators.
			if (String.IsNullOrEmpty(path.Replace(this.PSDriveInfo.Root, string.Empty)) || 
				String.IsNullOrEmpty(path.Replace(this.PSDriveInfo.Root + DatabaseUtils.PATH_SEPARATOR, string.Empty))) {
				return true;
			} else { 
				return false;
			} 
		}
		
		
		/// <summary> 
		/// Separates the path into individual elements. 
		/// </summary> 
		/// <param name="path">The path to split.</param> 
		/// <returns>An array of path segments.</returns> 
		private string[] ChunkPath(string path) { 
			// Normalize the path before separating. 
			string normalPath = NormalizePath(path); 
			// Return the path with the drive name and first path  
			// separator character removed, split by the path separator. 
			string pathNoDrive = normalPath.Replace(PSDriveInfo.Root + DatabaseUtils.PATH_SEPARATOR, string.Empty); 
			return pathNoDrive.Split(DatabaseUtils.PATH_SEPARATOR.ToCharArray()); 
		}
		
	    /// <summary> 
		/// The Windows PowerShell engine calls this method when the Get-Item  
		/// cmdlet is run. 
		/// </summary> 
		/// <param name="path">The path to the item to return.</param> 
		protected override void GetItem(string path) {
			// Check to see if the supplied path is to a drive. 
			if (PathIsDrive(path)) {
				WriteItemObject(PSDriveInfo, path, true); 
				return;
			}
			
			// Get the table name and row information from the path and do  
			// the necessary actions. 
			string schemaName;
			string tableName;
			string key;
			
			PathType type = GetNamesFromPath(path, out schemaName, out tableName, out key); 
			switch (type) {
				case PathType.Schema :
					DatabaseSchemaInfo schema = GetSchemas().Where( s => String.Equals(schemaName, s.Name, StringComparison.OrdinalIgnoreCase)).First();
					WriteItemObject(schema, path, true);
					break;
				case PathType.Table : 
					DatabaseTableInfo table = GetTables().Where( s => String.Equals(tableName, s.Name, StringComparison.OrdinalIgnoreCase)).First();; 
					WriteItemObject(table, path, true);
					break;
				case PathType.Row : 
					DatabaseRowInfo row = GetRow(tableName, key);
					WriteItemObject(row, path, false);
					break;
				default : 
					ThrowTerminatingInvalidPathException(path); 
					break;
			}
		}
		
	    /// <summary> 
		/// Test to see if the specified path is syntactically valid. 
		/// </summary> 
		/// <param name="path">The path to validate.</param> 
		/// <returns>True if the specified path is valid.</returns> 
		protected override bool IsValidPath(string path) { 
			// Check to see if the path is null or empty. 
			if (String.IsNullOrEmpty(path)) { 
				return false; 
			}
			path = NormalizePath(path);
			string[] pathElements = path.Split(DatabaseUtils.PATH_SEPARATOR.ToCharArray());
			foreach (string element in pathElements) {
				if (element.Length == 0) {
					return false;
				}
			}
			return true;
		}
		
		/// <summary> 
		/// Adapts the path, making sure the correct path separator character is used. 
		/// </summary> 
		/// <param name="path">Path to normalize.</param> 
		/// <returns>Normalized path.</returns> 
		private string NormalizePath(string path) { 
			string result = path;
			
			if (!String.IsNullOrEmpty(path)) { 
				result = path.Replace("/", DatabaseUtils.PATH_SEPARATOR); 
			} 

			return result; 
		}
		
		/// <summary> 
		/// Ensures that the drive is removed from the specified path. 
		/// </summary> 
		/// <param name="path">Path from which drive needs to be removed</param> 
		/// <returns>Path with drive information removed</returns> 
		private string StripDriveFromPath(string path) {
			if (String.IsNullOrEmpty(path)) {
				return String.Empty;
			}
			
			string root;
			if (this.PSDriveInfo == null) { 
				root = String.Empty; 
			} else {
				root = this.PSDriveInfo.Root; 
			}

			if (path.Contains(root)) { 
				return path.Substring(path.IndexOf(root, StringComparison.OrdinalIgnoreCase) + root.Length); 
			}
			return path; 
		}
		
		/// <summary> 
		/// Returns the schena, table/view name and the row number from the path. 
		/// </summary> 
		/// <param name="path">Path to investigate.</param> 
		/// <param name="schemaName">Name of the schema as represented in the  
		/// path.</param>
		/// <param name="tableName">Name of the table as represented in the  
		/// path.</param> 
		/// <param name="key">Primary key value obtained from the path.</param> 
		/// <returns>What the path represents</returns> 
		private PathType GetNamesFromPath(string path, out string schemaName, out string tableName, out string key) { 
			PathType retVal = PathType.Invalid;
			key = null;
			tableName = null;
			schemaName = null;
			// Check to see if the path is a drive. 
			if (this.PathIsDrive(path)) {
				return PathType.Database;
			}
			// Separate the path into parts. 
			string[] pathChunks = this.ChunkPath(path); 
			switch (pathChunks.Length) {
				case 3: {
					key = pathChunks[2];
					retVal = PathType.Row;
					goto case 2;
				}
				case 2: {
					string name = pathChunks[1];
					if (!TableNameIsValid(name)) { 
						return PathType.Invalid;
					}
					tableName = name;
					retVal = PathType.Table;
					goto case 1;
				}
				case 1: {
					string name = pathChunks[0];
					if (!SchemaNameIsValid(name)) { 
						return PathType.Invalid;
					}
					schemaName = name;
					retVal = PathType.Schema;
					break;
				}
				default: { 
					WriteError(new ErrorRecord( 
						new ArgumentException("The path supplied has too many segments"), 
							"PathNotValid", 
								ErrorCategory.InvalidArgument, 
								path)); 
					break;
				}
			}
			return retVal; 
		}
		
	    /// <summary> 
		/// Checks to see if the table name is valid. 
		/// </summary> 
		/// <param name="tableName">Table name to validate</param> 
		/// <remarks>Helps to check for SQL injection attacks</remarks> 
		/// <returns>A Boolean value indicating true if the name is valid.</returns> 
		private bool TableNameIsValid(string tableName) 
		{ 
			if (!DatabaseUtils.NameIsValid(tableName)) {
				WriteError(new ErrorRecord( 
									new ArgumentException("Table name not valid"),  
									"TableNameNotValid", 
									ErrorCategory.InvalidArgument,  
									tableName)); 
				return false;
			} 
			return true;
		}
		
		
		/// <summary> 
		/// Checks to see if the schema name is valid. 
		/// </summary> 
		/// <param name="schemaName">Schema name to validate</param> 
		/// <remarks>Helps to check for SQL injection attacks</remarks> 
		/// <returns>A Boolean value indicating true if the name is valid.</returns> 
		private bool SchemaNameIsValid(string schemaName) { 
			if (!DatabaseUtils.NameIsValid(schemaName)) {
				WriteError(new ErrorRecord(
									 new ArgumentException("Schema name not valid"),  
									 "TableNameNotValid", 
									 ErrorCategory.InvalidArgument,  
									 schemaName)); 
				return false;
			} 
			return true;
		}
		
		/// TODO Rewrite
		/// <summary> 
		/// Retrieve the list of tables from the database. 
		/// </summary> 
		/// <returns> 
		/// Collection of DatabaseTableInfo objects, each object representing 
		/// information about one database table
		/// </returns> 
		private IEnumerable<DatabaseTableInfo> GetTables() 
		{ 
			ICollection<DatabaseTableInfo> results = new List<DatabaseTableInfo>();
			// Using the OleDb connection to the database get the schema of tables. 
			DatabaseDriveInfo di = this.PSDriveInfo as DatabaseDriveInfo;
			if (di == null) {
				return null; 
			}
			OleDbConnection connection = di.DatabaseConnection;
			DataTable dt = connection.GetSchema("Tables");
			int count;
	 
			// Iterate through all the rows in the schema and create DatabaseTableInfo 
			// objects which represents a table. 
			foreach (DataRow dr in dt.Rows) { 
				string tableName = dr["TABLE_NAME"] as string;
				DataColumnCollection columns = null;

				// Find the number of rows in the table.
				try {
					string cmd = String.Format("Select count(*) from {0}", tableName);
					OleDbCommand command = new OleDbCommand(cmd, connection);
					count = (int)command.ExecuteScalar(); 
				} catch {
					count = 0; 
				}
				// Create the DatabaseTableInfo object representing the table. 
				DatabaseTableInfo table = new DatabaseTableInfo(dr, tableName, count, columns); 
				results.Add(table); 
			}
			return results; 
		}
		
		/// <summary> 
		/// Return all schemas information. 
		/// </summary> 
		/// <returns>Collection of schema information objects.</returns> 
		private IEnumerable<DatabaseSchemaInfo> GetSchemas() 
		{ 
			return new List<DatabaseSchemaInfo>() { schemaName };
		}
		
		/// <summary> 
		/// Return row information from a specified table. 
		/// </summary> 
		/// <param name="tableName">The name of the database table from  
		/// which to retrieve rows.</param> 
		/// <returns>Collection of row information objects.</returns> 
		private IEnumerable<DatabaseRowInfo> GetRows(string tableName) {
			ICollection<DatabaseRowInfo> results = new List<DatabaseRowInfo>();
			// Obtain the rows in the table and add them to the collection. 
			try {
				OleDataAdapter da = GetAdapterForTable(tableName);
				if (da == null) { 
				  return null; 
				}
				DataSet ds = GetDataSetForTable(da, tableName); 
				DataTable table = GetDataTable(ds, tableName); 
				foreach (DataRow row in table.Rows) { 
					results.Add(new DatabaseRowInfo(row)); 
				}
			} catch (Exception e) { 
				WriteError(new ErrorRecord(e, "CannotAccessSpecifiedRows", ErrorCategory.InvalidOperation, tableName)); 
			}
			return results; 
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
			WriteError(new ErrorRecord(new ItemNotFoundException(), "RowNotFound", ErrorCategory.ObjectNotFound, row.ToString(CultureInfo.CurrentCulture))); 
			return null; 
		}
		
		/// <summary> 
		/// Obtain a data adapter for the specified Table 
		/// </summary> 
		/// <param name="tableName">Name of the table to obtain the  
		/// adapter for</param> 
		/// <returns>Adapter object for the specified table</returns> 
		/// <remarks>An adapter serves as a bridge between a DataSet (in memory 
		/// representation of table) and the data source</remarks> 
		private OleDbDataAdapter GetAdapterForTable(string tableName) 
		{ 
			OleDbDataAdapter da = null; 
			DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
			
			if (di == null || !this.TableNameIsValid(tableName) || !this.TableIsPresent(tableName)) { 
				return null;
			} 
	 
			OleDbConnection connection = di.Connection; 

			try { 
				// Create an OleDb data adapter. This can be used to update the 
				// data source with the records that will be created here 
				// using data sets. 
				string sql = String.Format("Select * from {0}",tableName);
				da = new OleDbDataAdapter(new OleDbCommand(sql, connection)); 

				// Create an OleDb command builder object. This will create sql 
				// commands automatically for a single table, thus 
				// eliminating the need to create new sql statements for  
				// every operation to be done. 
				OleDbCommandBuilder cmd = new OleDbCommandBuilder(da); 

				// Set the delete command for the table here. 
				sql = String.Format("Delete from {0} where ID = ?", tableName);
				da.DeleteCommand = new OleDbCommand(sql, connection);

				// Specify a DeleteCommand parameter based on the "ID"  
				// column. 
				da.DeleteCommand.Parameters.Add(new OleDbParameter()); 
				da.DeleteCommand.Parameters[0].SourceColumn = "ID"; 

				// Create an InsertCommand based on the sql string 
				// Insert into "tablename" values (?,?,?)" where 
				// ? represents a column in the table. Note that  
				// the number of ? will be equal to the number of  
				// columns. 
				DataSet ds = new DataSet(); 

				da.FillSchema(ds, SchemaType.Source); 
				ds.Locale = CultureInfo.InvariantCulture; 

				sql = "Insert into " + tableName + " values ( "; 
				for (int i = 0; i < ds.Tables["Table"].Columns.Count; i++) 
				{ 
				  sql += "?, "; 
				} 

				sql = sql.Substring(0, sql.Length - 2);
				sql += ")";
				da.InsertCommand = new OleDbCommand(sql, connection); 

				// Create parameters for the InsertCommand based on the 
				// captions of each column. 
				for (int i = 0; i < ds.Tables["Table"].Columns.Count; i++) { 
					da.InsertCommand.Parameters.Add(new OleDbParameter()); 
					da.InsertCommand.Parameters[i].SourceColumn = ds.Tables["Table"].Columns[i].Caption; 
				} 

				// Open the connection if it is not already open.                  
				if (connection.State != ConnectionState.Open) { 
					connection.Open(); 
				}
			} 
			catch (Exception e) 
			{ 
				WriteError(new ErrorRecord(e, "CannotAccessSpecifiedTable", ErrorCategory.InvalidOperation, tableName)); 
			} 

			return da; 
		}
		
		#endregion Item Methods
		
		private void ThrowTerminatingInvalidPathException(string path) 
		{
			throw new ArgumentException(String.Format("Path must represent either a table or a row : {0}", path)); 
		}
    }
}