using System;
using System.Data;

namespace PowerShellDBDrive {
	/// <summary> 
	/// Contains information specific to the database table. 
	/// Similar to the DirectoryInfo class. 
	/// </summary> 
	public class DatabaseColumnInfo 
	{
		/// <summary> 
		/// The name of a schema. 
		/// </summary> 
		public string SchemaName { get; set; }
		
		/// <summary> 
		/// The name of a table. 
		/// </summary> 
		public string TableName { get; set; }
		
		/// <summary> 
		/// The name of a column. 
		/// </summary> 
		public string Name { get; set; }

		/// <summary> 
		/// The datatype
		/// </summary> 
		public string DataType { get; set; }

		/// <summary> 
		/// The length
		/// </summary> 		
		public int? Length { get; set; }

		/// <summary> 
		/// The precision
		/// </summary> 		
		public int? Precision { get; set; }

		/// <summary> 
		/// The scale
		/// </summary> 		
		public int? Scale { get; set; }

		/// <summary> 
		/// The nullable
		/// </summary> 		
		public bool Nullable { get; set; }

		/// <summary> 
		/// Initializes a new instance of the DatabaseTableInfo class. 
		/// </summary> 
		/// <param name="row">The row definition.</param> 
		/// <param name="name">The table name.</param> 
		/// <param name="rowCount">The number of rows in the table.</param> 
		/// <param name="columns">Information on the column tables.</param> 
		public DatabaseColumnInfo(string schemaName, string tableName, string columnName, string dataType, int? length, int? precision, int? scale, bool nullable) { 
			this.SchemaName = schemaName;
			this.TableName = tableName;
			this.Name = columnName;
			this.DataType = dataType;
			this.Length = length;
			this.Precision = precision;
			this.Scale = scale;
			this.Nullable = nullable;
		}
	}
}