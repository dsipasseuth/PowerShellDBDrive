using System;
using System.Linq;
using System.Collections.Generic;
using System.Management.Automation;
using System.Management.Automation.Provider;
using System.Data;
using System.Data.Common;

namespace PowerShellDBDrive.Provider
{
    [CmdletProvider( "DatabaseProvider", ProviderCapabilities.None )]
    public class DatabaseProvider : NavigationCmdletProvider 
    {
		public const int DEFAULT_PS
		
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
			
			if (drive.Root == null) {
				WriteError(new ErrorRecord(new ArgumentNullException("drive.Root"),"NullRoot",ErrorCategory.InvalidArgument,null));
				return null;
			}
			var driveParams = this.DynamicParameters as DatabaseParameters;
			var driveInfo = new DatabaseDriveInfo(drive, driveParams);
            DbProviderFactory factory = DbProviderFactories.GetFactory(driveParams.Provider);
            DbConnectionStringBuilder connectionStringBuilder = factory.CreateConnectionStringBuilder();
            connectionStringBuilder.ConnectionString = driveParams.ConnectionString;
            WriteDebug("Connection Information");
            foreach (string key in connectionStringBuilder.Keys)
            {
                WriteDebug(String.Format("{0} : {1}", key, connectionStringBuilder[key]));
            }
			driveInfo.DatabaseConnection = factory.CreateConnection();
            driveInfo.DatabaseConnection.ConnectionString = connectionStringBuilder.ConnectionString;
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
				WriteError(new ErrorRecord(new ArgumentNullException("drive"), "NullDrive", ErrorCategory.InvalidArgument, drive));
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
			
			DatabaseDriveInfo di = this.PSDriveInfo as DatabaseDriveInfo;
            if (di == null)
            {
                return;
            }
			
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
				case PathType.Database : 
					PSObject[] schemas = di.GetSchemas();
					WriteItemObject(schemas, path, true);
					break;
				case PathType.Schema :
					PSObject[] tables = di.GetTables(schemaName);
					WriteItemObject(tables, path, true);
					break;
				case PathType.Table : 
					PSObject[] rows = di.GetRows(tableName);
					WriteItemObject(rows, path, true);
					break;
				case PathType.Row : 
					DatabaseRowInfo row = di.GetRow(tableName, key);
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
            WriteDebug(String.Format("StripDriveFromPath:{0}", path));
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
            WriteDebug(String.Format("GetNamesFromPath:{0}", path));
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
		
		#endregion Item Methods
		
		private void ThrowTerminatingInvalidPathException(string path) 
		{
			throw new ArgumentException(String.Format("Path must represent either a table or a row : {0}", path)); 
		}
    }
}