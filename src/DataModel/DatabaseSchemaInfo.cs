using System; 
using System.Data;

namespace PowerShellDBDrive {
	/// <summary> 
	/// Contains information specific to an individual schema.
	/// Analogous to the FileInfo class. 
	/// </summary> 
	public class DatabaseSchemaInfo 
	{ 
		/// <summary> 
		/// User name
		/// </summary> 
		public string Name {get; set;}
		
		/// <summary> 
		/// Creation date time
		/// </summary> 
		public DateTime? CreateDate {get ; set; }
		
		/// <summary> 
		/// Initializes a new instance of the DatabaseSchemaInfo class. 
		/// </summary> 
		/// <param name="row">The row information.</param> 
		/// <param name="name">The row index.</param> 
		public DatabaseSchemaInfo(string name, DateTime? createdate) {
			this.Name = name;
			this.CreateDate = createdate;
		}
	}
}
 