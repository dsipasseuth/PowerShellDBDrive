using System.Data;

namespace PowerShellDBDrive {
	/// <summary> 
	/// Contains information specific to an individual table row. 
	/// Analogous to the FileInfo class. 
	/// </summary> 
	public class DatabaseSchemaInfo 
	{ 
		/// <summary> 
		/// The information about a row. 
		/// </summary> 
		public DataRow Data {get; set;}

		/// <summary> 
		/// Schema name
		/// </summary> 
		public string Name {get; set;}
		
		/// <summary> 
		/// The number of tables/views in a schema. 
		/// </summary> 
		public int Count { get; set; }
		
		/// <summary> 
		/// The number of tables/views in a schema. 
		/// </summary> 
		public int TableCount { get; set; }
		
		/// <summary> 
		/// The number of tables/views in a schema. 
		/// </summary> 
		public int ViewCount { get; set; }
		
		/// <summary> 
		/// Initializes a new instance of the DatabaseSchemaInfo class. 
		/// </summary> 
		/// <param name="row">The row information.</param> 
		/// <param name="name">The row index.</param> 
		public DatabaseSchemaInfo(DataRow row, string name, int count, int tableCount, int viewCount) { 
			this.Name = name; 
			this.Data = row;
			this.Count = count;
			this.TableCount = tableCount;
			this.ViewCount = viewCount;
		}
	}
}
 