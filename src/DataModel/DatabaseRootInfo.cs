using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PowerShellDBDrive.DataModel
{
    public class DatabaseRootInfo
    {
        public string Root { get; set; }

        public DatabaseRootInfo(string root)
        {
            this.Root = root;
        }
    }
}
