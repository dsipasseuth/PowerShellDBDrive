using System;
using System.Management.Automation;
using System.Data.OleDb;

namespace PowerShellDBDrive
{
    public class DatabaseDriveInfo : PSDriveInfo
    {
        public DatabaseDriveInfo( PSDriveInfo driveInfo, DatabaseParameters parameters ) : base( driveInfo )
        {
			
        }
		
		public OleDbConnection DatabaseConnection { get; set; }
    }
}