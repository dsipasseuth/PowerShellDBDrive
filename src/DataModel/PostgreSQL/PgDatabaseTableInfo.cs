using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PowerShellDBDrive.DataModel.PostgreSQL
{
    public class PgDatabaseTableInfo : IDatabaseTableInfo
    {
        public IDatabaseColumnInfo[] Columns { get; }

        public long RowCount { get; }

        public string SchemaName { get; }

        public string TableName { get; }

    }
}
