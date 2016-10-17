using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PowerShellDBDrive.DataModel.PostgreSQL
{
    public class PgDatabaseColumnInfo : IDatabaseColumnInfo
    {
        public string ColumnName { get; }

        public bool Nillable { get; }

        public string SchemaName { get; }

        public string TableName { get; }
    }
}
