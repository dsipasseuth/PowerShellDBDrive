using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PowerShellDBDrive.DataModel
{
    public interface IDatabaseColumnInfo
    {
        string SchemaName { get; }
        string TableName { get; }
        string ColumnName { get; }
        bool Nillable { get; }
    }
}
