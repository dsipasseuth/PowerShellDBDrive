using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PowerShellDBDrive.DataModel
{
    public interface IDatabaseTableInfo
    {
        string SchemaName { get; }
        string TableName { get; }
        IDatabaseColumnInfo[] Columns { get; }
        long RowCount { get; }
    }
}
