using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PowerShellDBDrive.DataModel.PostgreSQL
{
    public class PgDatabaseColumnInfo : IDatabaseColumnInfo
    {
        public string ColumnName { get; set; }

        public bool Nillable { get; set; }

        public string SchemaName { get; set; }

        public string TableName { get; set; }
    }
}
