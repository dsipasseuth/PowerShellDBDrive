using System.Data;

namespace PowerShellDBDrive {
	/// <summary> 
	/// Contains information specific to an individual table row. 
	/// Analogous to the FileInfo class. 
	/// </summary> 
	public class DatabaseRowInfo 
	{ 
		/// <summary> 
		/// The information about a row. 
		/// </summary> 
		public DataRow Data { get; set; }

		/// <summary> 
		/// Initializes a new instance of the DatabaseRowInfo class. 
		/// </summary> 
		/// <param name="row">The row information.</param> 
		/// <param name="key">The row index.</param> 
		public DatabaseRowInfo(DataRow row) { 
			this.Data = row;
		}
	}
}
 