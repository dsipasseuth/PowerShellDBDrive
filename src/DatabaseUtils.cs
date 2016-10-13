using System;
using System.Text.RegularExpressions;
using System.Management.Automation;
using System.Data.Common;

namespace PowerShellDBDrive {
	
	/// <summary> 
	/// Defines the types of paths to items. 
	/// {Database}/{Schema}/{Table}/{Row}
	/// </summary>
	public enum PathType 
	{ 
		/// <summary>
		/// Path to a database.
		/// Mostly will be the root for database.
		/// </summary>
		Database, 
		
		/// <summary>
		/// Path to a schema.
		/// </summary>
		Schema,
		
		/// <summary>
		/// Path to a table item.
		/// </summary>
		Table,
		
		/// <summary>
		/// Path to a row item.
		/// </summary>
		Row,
		
		/// <summary>
		/// A path to an item that is not a database, table, or row.
		/// </summary>
		Invalid 
	}
	
	public static class DatabaseUtils
    {
		/// <summary>
		/// Characters used to valid names.
		/// </summary> 
		public const string VALIDATION_PATTERN = @"^[a-zA-Z0-9_]+$"; 
		 
		/// <summary> 
		/// The valid path separator character. 
		/// </summary>
		public const string PATH_SEPARATOR = "\\";
		
		/// <summary>
		/// Select string to be used
		/// </summary> 
		public const string SELECT_STRING_FORMAT = "Select * From {0}";
		
		/// <summary> 
		/// Checks to see if the object name is valid. 
		/// </summary> 
		/// <param name="databaseObject">Object name to validate</param> 
		/// <remarks>Helps to check for SQL injection attacks</remarks> 
		/// <returns>A Boolean value indicating true if the name is valid.</returns> 
		public static bool NameIsValid(string databaseObject) {
			Regex exp = new Regex(VALIDATION_PATTERN, RegexOptions.Compiled | RegexOptions.IgnoreCase); 
			if (exp.IsMatch(databaseObject)) { 
				return true;
			}
			return false; 
		}
		
		/// <summary> 
		/// Create OleDbCommand object for select query
		/// </summary> 
		/// <param name="tableName">table to query on</param> 
		/// <returns>a new OleDbCommand without connection associated.</returns> 
		public static string GetSelectStringForTable(string tableName) {
			return String.Format(SELECT_STRING_FORMAT, tableName);
		}
		
		/// <summary> 
		/// Ensures that the drive is removed from the specified path. 
		/// </summary> 
		/// <param name="path">Path from which drive needs to be removed</param> 
		/// <returns>Path with drive information removed</returns> 
		public static string RemoveDriveFromPath(string path, string root) 
		{ 
			string result = path;
			if (result == null) 
			{ 
				result = String.Empty; 
			} 
			if (result.Contains(root)) 
			{ 
				result = result.Substring(result.IndexOf(root, StringComparison.OrdinalIgnoreCase) + root.Length); 
			}
			return result; 
		}
		
		/// <summary> 
		/// Adapts the path, making sure the correct path separator character is used. 
		/// </summary> 
		/// <param name="path">Path to normalize.</param> 
		/// <returns>Normalized path.</returns> 
		public static string NormalizePath(string path) { 
			string result = path;
			if (!String.IsNullOrEmpty(path)) { 
				result = path.Replace("/", PATH_SEPARATOR); 
			}
			return result; 
		}
		
		/// <summary> 
		/// Separates the path into individual elements. 
		/// </summary> 
		/// <param name="path">The path to split.</param> 
		/// <returns>An array of path segments.</returns> 
		public static string[] ChunkPath(string root, string path) { 
			// Normalize the path before separating. 
			string normalPath = NormalizePath(path); 
			// Return the path with the drive name and first path  
			// separator character removed, split by the path separator. 
			string pathNoDrive = normalPath.Replace(root + PATH_SEPARATOR, string.Empty); 
			return pathNoDrive.Split(PATH_SEPARATOR.ToCharArray());
		}
		
		/// <summary>
		/// Checks to see if a given path is actually a drive name. 
		/// </summary>
		/// <param name="path">The path to investigate.</param> 
		/// <returns>
		/// True if the path represents a drive; otherwise false is returned. 
		/// </returns>
		public static bool PathIsDrive(string root, string path) {
			if (string.IsNullOrEmpty(root)) {
				if (string.IsNullOrEmpty(path)) {
					return true;
				} else {
					return false;
				}
			}
			// Remove the drive name and first path separator.  If the
			// path is reduced to nothing, it is a drive. Also if it is
			// just a drive then there will not be any path separators.
			if (String.IsNullOrEmpty(path.Replace(root, string.Empty)) || 
				String.IsNullOrEmpty(path.Replace(root + DatabaseUtils.PATH_SEPARATOR, string.Empty))) {
				return true;
			} else {
				return false;
			}
		}
	}
	
	/// <summary> 
	/// Builder for PSObject.
	/// </summary> 
	public class PSObjectBuilder {
		
		private PSObject currentInstance;
		
		public void NewInstance() {
			currentInstance = new PSObject();
		}
		
		public void AddField(String fieldName, Object fieldValue, Type type) {
			System.TypeCode typeCode = Type.GetTypeCode(type);
			switch(typeCode) {
				case TypeCode.Boolean : 
					AddField(fieldName, (bool) fieldValue);
					break;
				default : 
					if (fieldValue != null) {
						AddField(fieldName, fieldValue.ToString());
					} else {
						AddField(fieldName, null);
					}
					break;
			}
		}
		
		public void AddField(String fieldName, String fieldValue) {
			currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue));
		}
		
		public void AddField(String fieldName, bool fieldValue) {
			currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue));
		}
		
		public void AddField(String fieldName, int fieldValue) {
			currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue));
		}
		
		public void AddField(String fieldName, double fieldValue) {
			currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue));
		}
		
		public PSObject Build() {
			return currentInstance;
		}
	}
}