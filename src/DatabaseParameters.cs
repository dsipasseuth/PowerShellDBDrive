using System;
using System.Management.Automation;

namespace PowerShellDBDrive
{
    public class DatabaseParameters
    {
        public DatabaseParameters() {
		}
		
		[Parameter(Mandatory=true)]
		public string ConnectionString { get; set; }

        [Parameter(Mandatory=true)]
        public string Provider { get; set; }
	}
}