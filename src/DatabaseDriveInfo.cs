using System.Management.Automation;
using System.Data.Common;

namespace PowerShellDBDrive
{
    public class DatabaseDriveInfo : PSDriveInfo
    {
        public DatabaseDriveInfo( PSDriveInfo driveInfo, DatabaseParameters parameters ) : base( driveInfo )
        {
			
        }
		
		public DbConnection DatabaseConnection { get; set; }
    }
}