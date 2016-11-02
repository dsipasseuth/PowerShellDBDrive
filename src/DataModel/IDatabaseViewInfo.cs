using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PowerShellDBDrive.DataModel
{
    public interface IDatabaseViewInfo
    {
        string SchemaName { get; }
        string ViewName { get; }
        IDatabaseColumnInfo[] Columns { get; }
    }
}
