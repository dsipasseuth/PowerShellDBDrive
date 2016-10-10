using System;
using System.Text.RegularExpressions;
using System.Data.OleDb;
using System.Management.Automation;

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
		public static OleDbCommand GetSelectStringForTable(string tableName) {
			return new OleDbCommand(String.Format(SELECT_STRING_FORMAT, tableName));
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
		
		public void addField(String fieldName, Object fieldValue) {
			if (fieldValue == null) {
				currentInstance.Members.Add(new PSNoteProperty(fieldName, String.Empty));
			} else if (fieldValue as string == null) {
				currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue as string));
			}
		}
		
		public void addField(String fieldName, String fieldValue) {
			currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue));
		}
		
		public void addField(String fieldName, int fieldValue) {
			currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue));
		}
		
		public void addField(String fieldName, double fieldValue) {
			currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue));
		}
		
		public PSObject Build() {
			return currentInstance;
		}
	}
}