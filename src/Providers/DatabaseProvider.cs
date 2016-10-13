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
		/// <summary> 
		/// Default size of array written out to PowerShell.
		/// </summary> 
		public const int DEFAULT_PS_OUTPUT_ARRAY_SIZE = 50;
		
		public int PsOutputArraySize { get; set; }
		
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
			
			if (drive.Root == null || String.IsNullOrEmpty(drive.Root)) {
				WriteError(new ErrorRecord(new ArgumentNullException("drive.Root"),"NullRoot",ErrorCategory.InvalidArgument,null));
				return null;
			}
			var driveParams = this.DynamicParameters as DatabaseParameters;
			PsOutputArraySize = DEFAULT_PS_OUTPUT_ARRAY_SIZE;
			DatabaseDriveInfo driveInfo = new DatabaseDriveInfo(drive, driveParams);
			WriteDebug(String.Format("Parsed Connection String : {0}", driveInfo.ParsedConnectionString));
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
			DatabaseDriveInfo driveInfo = drive as DatabaseDriveInfo;
			if (driveInfo == null) {
				return null;
			}
			return driveInfo;
		}
		
        protected override object NewDriveDynamicParameters() {
            return new DatabaseParameters();
        }
		
		#endregion Drive Manipulation
		
		#region Item Methods
		
	    /// <summary> 
		/// The Windows PowerShell engine calls this method when the Get-Item  
		/// cmdlet is run. 
		/// </summary> 
		/// <param name="path">The path to the item to return.</param> 
		protected override void GetItem(string path) {
			WriteVerbose(string.Format("GetItem: <- Path='{0}'", path));
			DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
            if (di == null)
            {
                return;
            }
			
			string schemaName;
			string tableName;
			string key;
			
			PathType type = GetNamesFromPath(path, out schemaName, out tableName, out key);
			switch (type) {
				case PathType.Database:
					WriteVerbose("GetItem: -> Database");
					WriteItemObject(PSDriveInfo, path, true);
					break;
				case PathType.Schema:
					DatabaseSchemaInfo schema = di.GetSchema(schemaName);
					WriteVerbose("GetItem: -> Schema");
					WriteItemObject(schema, path, true);
					break;
				case PathType.Table: 
					DatabaseTableInfo table = di.GetTable(schemaName, tableName);
					WriteVerbose("GetItem: -> Table");
					WriteItemObject(table, path, true);
					break;
				case PathType.Row:
					PSObject row = di.GetRow(schemaName, tableName, key);
					WriteVerbose("GetItem: -> Row");
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
			WriteVerbose(string.Format("IsValidPath:{0}", path));
			if (string.IsNullOrEmpty(path)) { 
				return false; 
			}
			path = DatabaseUtils.NormalizePath(path);
			string[] pathElements = path.Split(DatabaseUtils.PATH_SEPARATOR.ToCharArray());
			foreach (string element in pathElements) {
				if (element.Length == 0) {
					return false;
				}
			}
			return true;
		}
		
		/// <summary> 
		/// Ensures that the drive is removed from the specified path. 
		/// </summary> 
		/// <param name="path">Path from which drive needs to be removed</param> 
		/// <returns>Path with drive information removed</returns> 
		private string StripDriveFromPath(string path) {
            WriteVerbose(string.Format("StripDriveFromPath:{0}", path));
			if (string.IsNullOrEmpty(path)) {
				return string.Empty;
			}
			
			string root;
			if (this.PSDriveInfo == null) { 
				root = string.Empty; 
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
            WriteVerbose(string.Format("GetNamesFromPath:'{0}' , '{1}'", PSDriveInfo.Root, path));
			PathType retVal = PathType.Invalid;
			key = null;
			tableName = null;
			schemaName = null;
			// Check to see if the path is a drive. 
			if (DatabaseUtils.PathIsDrive(PSDriveInfo.Root, path)) {
				return PathType.Database;
			}
			// Separate the path into parts. 
			string[] pathChunks = DatabaseUtils.ChunkPath(PSDriveInfo.Root, path); 
			WriteVerbose(string.Join(" ", pathChunks));
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
		/// Test to see if the specified item exists. 
		/// </summary> 
		/// <param name="path">The path to the item to verify.</param> 
		/// <returns>True if the item is found.</returns> 
		protected override bool ItemExists(string path) 
		{
			WriteVerbose(string.Format("ItemExists: <- Path='{0}'", path));
			DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
			if (di == null) {
				WriteVerbose("ItemExists: -> false");
				return false;
			}
			
			string schemaName;
			string tableName; 
			string key; 

			PathType type = GetNamesFromPath(path, out schemaName, out tableName, out key); 
			switch(type) {
				case PathType.Database:
					WriteVerbose("ItemExists: {PathType.Database} -> true");
					return true;
				case PathType.Schema:
					WriteVerbose("ItemExists: {PathType.Schema} -> true");
					return true;
				case PathType.Table:
					WriteVerbose("ItemExists: {PathType.Table} -> true");
					return true;
				case PathType.Row : 
					WriteVerbose("ItemExists: {PathType.Row} -> false");
					return false;
				default : 
					WriteVerbose("ItemExists: {PathType.Invalid} -> false");
					return false;
			}
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
		
		#region Container Methods
		
		/// <summary> 
		/// The Windows PowerShell engine calls this method when the Get-ChildItem  
		/// cmdlet is run. This provider returns either the tables in the database  
		/// or the rows of the table. 
		/// </summary> 
		/// <param name="path">The path to the parent item.</param> 
		/// <param name="recurse">A Boolean value that indicates true to return all  
		/// child items recursively. 
		/// </param> 
		protected override void GetChildItems(string path, bool recurse)
		{
			WriteVerbose(string.Format("GetChildItems: <- Path='{0}', Recurse='{1}'", path, recurse));
			DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
            if (di == null)
            {
                return;
            }
			
			// Get the table name, row number, and the path type from the path. 
			string schemaName;
			string tableName;
			string key;

			PathType type = GetNamesFromPath(path, out schemaName, out tableName, out key); 
			
			switch(type) {
				case PathType.Database : 
					WriteVerbose("GetChildItems: -> Database");
					foreach (DatabaseSchemaInfo schema in di.GetSchemas()) 
					{
						WriteVerbose(string.Format("GetChildItems: ---> Database schema '{0}'", schema.Name));
						WriteItemObject(schema, path, true);
						if (recurse) 
						{
							GetChildItems(path + DatabaseUtils.PATH_SEPARATOR + schema.Name, recurse);
						}
					}
					break;
				case PathType.Schema :
					WriteVerbose("GetChildItems: -> Schema");
					foreach (DatabaseTableInfo table in di.GetTables(schemaName)) 
					{
						WriteVerbose(string.Format("GetChildItems: ---> Database table '{0}'", table.Name));
						WriteItemObject(table, path, true);
						if (recurse)
						{
							GetChildItems(path + DatabaseUtils.PATH_SEPARATOR + table.Name, recurse);
						}
					}
					break;
				case PathType.Table :
					WriteVerbose("GetChildItems: -> Table");
					foreach (PSObject row in di.GetRows(schemaName, tableName)) 
					{
						WriteItemObject(row, path, false);
					}
					break;
				case PathType.Row :
						
					break;
				default : 
					ThrowTerminatingInvalidPathException(path);
					break;
			}
		}
		
		/// <summary> 
		/// Return the names of all child items. 
		/// </summary> 
		/// <param name="path">The root path.</param> 
		/// <param name="returnContainers">This parameter is not used.</param> 
		protected override void GetChildNames(string path, ReturnContainers returnContainers) 
		{
			WriteVerbose(string.Format("GetChildNames: <- Path='{0}'", path));
			DatabaseDriveInfo di = PSDriveInfo as DatabaseDriveInfo;
			if (di == null) {
				return;
			}
			
			// Get type, table name and row number from the path. 
			string schemaName;
			string tableName; 
			string key; 
	 
			PathType type = GetNamesFromPath(path, out schemaName, out tableName, out key); 
			switch(type) {
				case PathType.Database: 
					foreach (DatabaseSchemaInfo schema in di.GetSchemas()) 
					{
						WriteItemObject(schema.Name, path, false); 
					}
					break;
				case PathType.Schema:
					foreach (DatabaseTableInfo table in di.GetTables(schemaName)) 
					{
						WriteItemObject(table.Name, path, false); 
					}
					break;
				case PathType.Table:
					foreach (PSObject row in di.GetRows(schemaName, tableName)) 
					{ 
						/// TODO WriteItemObject(row.Properties[], path, false); 
					}
					break;
				case PathType.Row:
					break;
				default:
					ThrowTerminatingInvalidPathException(path); 
					break;
			}
		}
		
	    /// <summary> 
		/// Determines if the specified path has child items. 
		/// </summary> 
		/// <param name="path">The path to examine.</param> 
		/// <returns> 
		/// True if the specified path has child items. 
		/// </returns> 
		protected override bool HasChildItems(string path) 
		{
			WriteVerbose(string.Format("HasChildItems: <- Path='{0}'", path));
			return DatabaseUtils.ChunkPath(PSDriveInfo.Root, path).Length < 3;
		}
		
		#endregion Container Methods
		
		#region Navigation Methods
		
		
		/// <summary> 
		/// Determine if the path specified is that of a container. 
		/// </summary> 
		/// <param name="path">The path to check.</param> 
		/// <returns>True if the path specifies a container.</returns> 
		protected override bool IsItemContainer(string path) 
		{
			WriteVerbose(string.Format("IsItemContainer: <- Path='{0}'", path));
			string schemaName;
			string tableName;
			string key;
			PathType type = GetNamesFromPath(path, out schemaName, out tableName, out key); 
			if (type == PathType.Row || type == PathType.Invalid) {
				WriteVerbose("IsItemContainer: -> (PathType.Row / PathType.Invalid) false");
				return false;
			}
			WriteVerbose("IsItemContainer: -> true");
			return true;
		}
		
		/// <summary> 
		/// Gets the name of the leaf element in the specified path.         
		/// </summary> 
		/// <param name="path"> 
		/// The full or partial provider specific path. 
		/// </param> 
		/// <returns> 
		/// The leaf element in the path. 
		/// </returns> 
		protected override string GetChildName(string path) 
		{
			WriteVerbose(string.Format("GetChildName: <- Path='{0}'", path));
			string schemaName;
			string tableName;
			string key;

			PathType type = GetNamesFromPath(path, out schemaName, out tableName, out key); 
			switch(type) {
				case PathType.Database:
					return path;
				case PathType.Schema: 
					return schemaName;
				case PathType.Table: 
					return tableName;
				case PathType.Row: 
					return key;
				default : 
					ThrowTerminatingInvalidPathException(path); 
					break;
			}
			return null;
		} 
		
		/// <summary> 
		/// Returns the parent portion of the path, removing the child  
		/// segment of the path.  
		/// </summary> 
		/// <param name="path"> 
		/// A full or partial provider specific path. The path may be to an 
		/// item that may or may not exist. 
		/// </param> 
		/// <param name="root"> 
		/// The fully qualified path to the root of a drive. This parameter 
		/// may be null or empty if a mounted drive is not in use for this 
		/// operation.  If this parameter is not null or empty the result 
		/// of the method should not be a path to a container that is a 
		/// parent or in a different tree than the root. 
		/// </param> 
		/// <returns>The parent portion of the path.</returns> 
		protected override string GetParentPath(string path, string root) 
		{
			// If the root is specified then the path has to contain 
			// the root. If not nothing should be returned. 
			WriteVerbose(string.Format("GetParentPath: <- Path='{0}', Root='{1}'", path, root));
			if (!string.IsNullOrEmpty(root) && !path.Contains(root)) 
			{
				return root;
			}
			if (string.IsNullOrEmpty(path) || !path.Contains(DatabaseUtils.PATH_SEPARATOR)) 
			{
				return root;
			}
			return path.Substring(0, path.LastIndexOf(DatabaseUtils.PATH_SEPARATOR, StringComparison.OrdinalIgnoreCase));
		}
		
		 
		/// <summary> 
		/// Normalizes the path so that it is a relative path to the  
		/// basePath that was passed. 
		/// </summary> 
		/// <param name="path"> 
		/// A fully qualified provider specific path to an item.  The item 
		/// should exist or the provider should write out an error. 
		/// </param> 
		/// <param name="basepath"> 
		/// The path that the return value should be relative to. 
		/// </param> 
		/// <returns> 
		/// A normalized path that is relative to the basePath that was 
		/// passed. The provider should parse the path parameter, normalize 
		/// the path, and then return the normalized path relative to the 
		/// basePath. 
		/// </returns> 
		protected override string NormalizeRelativePath(string path, string basepath) 
		{
			WriteVerbose(string.Format("NormalizeRelativePath: <- Path='{0}', Basepath='{1}'", path, basepath));
			// Normalize the paths first. 
			string normalPath = DatabaseUtils.NormalizePath(path); 
			normalPath = DatabaseUtils.RemoveDriveFromPath(normalPath, PSDriveInfo.Root); 
			string normalBasePath = DatabaseUtils.NormalizePath(basepath); 
			normalBasePath = DatabaseUtils.RemoveDriveFromPath(normalBasePath, PSDriveInfo.Root); 
	 
			if (string.IsNullOrEmpty(normalBasePath)) 
			{ 
				return normalPath; 
			} 
			else 
			{ 
				if (!normalPath.Contains(normalBasePath)) 
				{ 
					return null; 
				} 
				return normalPath.Substring(normalBasePath.Length + DatabaseUtils.PATH_SEPARATOR.Length); 
			}
		}
		
		
		/// <summary> 
		/// Joins two strings with a provider specific path separator. 
		/// </summary> 
		/// <param name="parent"> 
		/// The parent segment of a path to be joined with the child. 
		/// </param> 
		/// <param name="child"> 
		/// The child segment of a path to be joined with the parent. 
		/// </param> 
		/// <returns> 
		/// A string that contains the parent and child segments of the path 
		/// joined by a path separator. 
		/// </returns> 
		protected override string MakePath(string parent, string child) 
		{ 
			WriteVerbose(string.Format("MakePath: <- Parent='{0}', Child='{1}'", parent, child));
			string result;
			string normalParent = DatabaseUtils.NormalizePath(parent); 
			normalParent = DatabaseUtils.RemoveDriveFromPath(normalParent, PSDriveInfo.Root); 
			string normalChild = DatabaseUtils.NormalizePath(child); 
			normalChild = DatabaseUtils.RemoveDriveFromPath(normalChild, PSDriveInfo.Root); 

			if (String.IsNullOrEmpty(normalParent) && String.IsNullOrEmpty(normalChild)) 
			{ 
				result = String.Empty; 
			} 
			else if (String.IsNullOrEmpty(normalParent) && !String.IsNullOrEmpty(normalChild)) 
			{ 
				result = normalChild; 
			} 
			else if (!String.IsNullOrEmpty(normalParent) && String.IsNullOrEmpty(normalChild)) 
			{ 
				if (normalParent.EndsWith(DatabaseUtils.PATH_SEPARATOR, StringComparison.OrdinalIgnoreCase)) 
				{ 
					result = normalParent; 
				}
				else 
				{ 
					result = normalParent + DatabaseUtils.PATH_SEPARATOR; 
				} 
			}  
			else 
			{ 
				if (!normalParent.Equals(String.Empty) && 
					!normalParent.EndsWith(DatabaseUtils.PATH_SEPARATOR, StringComparison.OrdinalIgnoreCase)) 
				{ 
				  result = normalParent + DatabaseUtils.PATH_SEPARATOR; 
				} 
				else 
				{ 
				  result = normalParent; 
				}

				if (normalChild.StartsWith(DatabaseUtils.PATH_SEPARATOR, StringComparison.OrdinalIgnoreCase)) 
				{ 
				  result += normalChild.Substring(1); 
				} 
				else 
				{ 
				  result += normalChild; 
				}
			}
			WriteVerbose(string.Format("MakePath: -> {0}", result));
			return result; 
		}
		
		#endregion Navigation Methods
		
		private void ThrowTerminatingInvalidPathException(string path) 
		{
			throw new ArgumentException(string.Format("Path must represent either a table or a row : {0}", path)); 
		}
    }
}