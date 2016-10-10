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
		/// Information about a row. 
		/// </summary> 
		public DataRow Data { get; set; }

		/// <summary> 
		/// The name of a table. 
		/// </summary> 
		public string Name { get; set; }

		/// <summary> 
		/// The number of rows in a table. 
		/// </summary> 
		public int RowCount { get; set; }

		/// <summary> 
		/// The column difinition of a table. 
		/// </summary> 
		public DataColumnCollection Columns { get; set; }

		/// <summary> 
		/// Initializes a new instance of the DatabaseTableInfo class. 
		/// </summary> 
		/// <param name="row">The row definition.</param> 
		/// <param name="name">The table name.</param> 
		/// <param name="rowCount">The number of rows in the table.</param> 
		/// <param name="columns">Information on the column tables.</param> 
		public DatabaseTableInfo(DataRow row, string name, int rowCount, DataColumnCollection columns) { 
			this.Name = name; 
			this.Data = row; 
			this.RowCount = rowCount; 
			this.Columns = columns; 
		}
	}
}