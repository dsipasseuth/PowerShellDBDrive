using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PowerShellDBDrive.DataModel.Oracle
{
    public class OracleDatabaseViewInfo : IDatabaseViewInfo
    {
        public string OidText { get; set; }
        public long? OidTextLength { get; set; }
        public string Owner { get; set; }
        public string SuperviewName { get; set; }
        public string Text { get; set; }
        public long? TextLength { get; set; }
        public string TypeText { get; set; }
        public long? TypeTextLength { get; set; }
        public string ViewType { get; set; }
        public string ViewTypeOwner { get; set; }

        public string ViewName { get; set; }

        public string SchemaName
        {
            get
            {
                return Owner;
            }
        }

        
    }
}
