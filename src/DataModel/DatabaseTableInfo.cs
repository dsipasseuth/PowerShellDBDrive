using System;
using System.Data;

namespace PowerShellDBDrive {
	/// <summary> 
	/// Contains information specific to the database table. 
	/// Similar to the DirectoryInfo class. 
	/// </summary> 
	public class DatabaseTableInfo 
	{
		/// <summary> 
		/// The name of a schema. 
		/// </summary> 
		public string SchemaName { get; set; }
		
		/// <summary> 
		/// The name of a table. 
		/// </summary> 
		public string Name { get; set; }

		/// <summary> 
		/// The number of rows in a table. 
		/// </summary> 
		public long RowCount { get; set; }
		
		/// <summary> 
		/// Definitions of columns.
		/// </summary> 
		public DatabaseColumnInfo[] Columns { get; set; }

		/// <summary> 
		/// Initializes a new instance of the DatabaseTableInfo class. 
		/// </summary> 
		/// <param name="row">The row definition.</param> 
		/// <param name="name">The table name.</param> 
		/// <param name="rowCount">The number of rows in the table.</param> 
		/// <param name="columns">Information on the column tables.</param> 
		public DatabaseTableInfo(string schemaName, string tableName, long rowCount, DatabaseColumnInfo[] columns) { 
			this.SchemaName = schemaName;
			this.Name = tableName;
			this.RowCount = rowCount;
			this.Columns = columns;
		}
	}
}