using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PowerShellDBDrive.DataModel.PostgreSQL
{
    public class PgDatabaseTableInfo : IDatabaseTableInfo
    {
        public IDatabaseColumnInfo[] Columns { get; set; }

        public long RowCount { get; set; }

        public string SchemaName { get; set; }

        public string TableName { get; set; }

    }
}
