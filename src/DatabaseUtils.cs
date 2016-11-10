using System;
using System.Text.RegularExpressions;
using System.Management.Automation;
using System.Linq;

namespace PowerShellDBDrive {
	
	/// <summary> 
	/// Defines the types of paths to items. 
	/// {Root}/{Database}/{Schema}/{ObjectType}/{Table|View}/{Row}
	/// </summary>
	public enum PathType 
	{ 
		/// Logical Root of database.
		Root,
		
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
		/// Path to object types availables.
		/// </summary>
		ObjectType,
		
		/// <summary>
		/// Path to an object item. (Can be a table, view, etc...)
		/// </summary>
		Object,
		
		/// <summary>
		/// Path to a row item.
		/// </summary>
		Row,
		
		/// <summary>
		/// A path to an item that is not a database, table, or row.
		/// </summary>
		Invalid 
	}
	
	/// <summary> 
	/// Define supported Object Types
	/// </summary>
	public enum ObjectType {
		///<summary>
		///Table object type
		///</summary>
		TABLE, 
		///<summary>
		///View object type
		///</summary>
		VIEW
	}

    /// <summary>
    /// A Path descriptor.
    /// </summary>
    public class PathDescriptor
    {
        /// <summary>
        /// Create a path descriptor from a full path (not partial)
        /// </summary>
        /// <param name="path">the full path with drive name</param>
        public PathDescriptor (string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                this.PathType = PathType.Database;
            }
            else
            {
                Match matcher = DatabaseUtils.PATH_VALIDATOR.Match(path);
                if (!matcher.Success)
                {
                    this.PathType = PathType.Invalid;
                    return;
                }
                /// Capturing groups follows
                Group group = matcher.Groups["PathElement"];
                CaptureCollection captureCollection = group.Captures;
                
                int count = captureCollection.Count;
                
                if (count == 0)
                {
                    this.PathType = PathType.Database;
                }
                if (count > 0)
                {
                    this.SchemaName = captureCollection[0].Value;
                    this.PathType = PathType.Schema;
                }
                if (count > 1)
                {
                    this.DatabaseObjectType = DatabaseUtils.ParseEnum<ObjectType>(captureCollection[1].Value);
                    this.PathType = PathType.ObjectType;
                }
                if (count > 2)
                {
                    this.PathType = PathType.Object;
                    this.ObjectPath = new string[count - 2];
                    for (int j = 0; j < count - 2; j++ )
                    {
                        this.ObjectPath[j] = captureCollection[2 + j].Value;
                    }
                }
            }
        }

        /// <summary>
        /// Path type 
        /// </summary>
        public PathType PathType { get; set; }

        /// <summary>
        /// The current database connected to.
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// The Schema Name
        /// </summary>
        public string SchemaName { get; set; }

        /// <summary>
        /// The Object Type.
        /// </summary>
        public ObjectType DatabaseObjectType { get; set; }

        /// <summary>
        /// The object path from object type.
        /// </summary>
        public string[] ObjectPath { get; set; }
    }
	
	public static class DatabaseUtils
    {
		/// <summary>
        /// Generic Matcher for path. (should match anything that is alpha numeric with underscore separated with single path separators) (DATABASE is OK, DATABASE\SCHEMA\TABLENAME\ should be OK too.
        /// </summary>
        public static readonly Regex PATH_VALIDATOR = new Regex(@"^([a-z0-9_]+:(\\|$))?((?<PathElement>[a-z0-9_]+)\\?)*$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public static readonly Regex NAME_VALIDATOR = new Regex(@"^[a-z0-9_]+$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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
			if (NAME_VALIDATOR.IsMatch(databaseObject)) { 
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
        /// Parse an string to given enum.
        /// </summary>
        /// <typeparam name="T">type of enum</typeparam>
        /// <param name="value">string value of enum</param>
        /// <returns>the enum type</returns>
        public static T ParseEnum<T>(string value)
        {
            return (T)Enum.Parse(typeof(T), value, true);
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
			if (result.StartsWith(root)) 
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
			if (!string.IsNullOrEmpty(path)) { 
				result = path.Replace("/", PATH_SEPARATOR); 
			}
			return result; 
		}
		
		/// <summary> 
		/// Separates the path into individual elements. 
		/// </summary> 
		/// <param name="path">The path to split.</param> 
		/// <param name="root">The Root.</param>
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
			if (string.IsNullOrEmpty(path.Replace(root, string.Empty)) || 
				string.IsNullOrEmpty(path.Replace(root + DatabaseUtils.PATH_SEPARATOR, string.Empty))) {
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
		
		public void AddField(string fieldName, Object fieldValue, Type type) {
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
		
		public void AddField(string fieldName, string fieldValue) {
			currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue));
		}
		
		public void AddField(string fieldName, bool fieldValue) {
			currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue));
		}
		
		public void AddField(string fieldName, int fieldValue) {
			currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue));
		}
		
		public void AddField(string fieldName, double fieldValue) {
			currentInstance.Members.Add(new PSNoteProperty(fieldName, fieldValue));
		}
		
		public PSObject Build() {
			return currentInstance;
		}
	}
}