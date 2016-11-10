using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PowerShellDBDrive.DataModel.PostgreSQL
{
    public class PgDatabaseTableInfo : IDatabaseTableInfo
    {

    	public string TableCatalog {get; set;}

		public string TableSchema {get;set;}

		public string TableType {get;set;}

		public string SelfReferencingColumnName {get;set;}

		public string ReferenceGeneration {get;set;}

		public string UserDefinedTypeCatalog {get;set;}

		public string UserDefinedTypeSchema {get;set;}

		public string UserDefinedTypeName {get;set;}

		public bool? IsInsertableInto {get;set;}

		public bool? IsTyped {get;set;}

		public string CommitAction {get;set;}

        public IDatabaseColumnInfo[] Columns { get; set; }

        public long RowCount { get; set; }

        public string SchemaName { get; set; }

        public string TableName { get; set; }

    }
}
