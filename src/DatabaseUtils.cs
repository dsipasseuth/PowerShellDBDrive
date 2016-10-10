using System;
using System.Management.Automation;
using System.Text.RegularExpressions;

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
	}
}