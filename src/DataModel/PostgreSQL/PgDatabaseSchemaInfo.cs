using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PowerShellDBDrive.DataModel.PostgreSQL
{
    public class PgDatabaseSchemaInfo : IDatabaseSchemaInfo
    {
        public string SchemaName { get; set; }
		
		public string SchemaOwner { get; set; }
		
		public string CatalogName { get; set; }
		
		public PgDatabaseSchemaInfo(string catalog, string schemaOwner, string schemaName) {
			this.SchemaName = schemaName;
			this.SchemaOwner = schemaOwner;
			this.CatalogName = catalog;
		}
    }
}
