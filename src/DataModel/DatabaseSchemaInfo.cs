using System; 
using System.Data;

namespace PowerShellDBDrive {
	/// <summary> 
	/// Contains information specific to an individual schema.
	/// Analogous to the FileInfo class. 
	/// </summary> 
	public class DatabaseSchemaInfo 
	{ 
		public long UserId { get; set; }
		/// <summary> 
		/// User name
		/// </summary> 
		public string SchemaName { get; set; }
		
		/// <summary> 
		/// User name
		/// </summary> 
		public DateTime CreateDate { get; set; }
		
		/// <summary> 
		/// Initializes a new instance of the DatabaseSchemaInfo class. 
		/// </summary>
		/// <param name="name">The row index.</param> 
		public DatabaseSchemaInfo(long userid, string name, DateTime createdate) {
			this.UserId = userid;
			this.SchemaName = name;
			this.CreateDate = createdate;
		}
	}
}
 