using System;
using System.Management.Automation;
using System.Management.Automation.Provider;
using PowerShellDBDrive;
using System.Data.OleDb;

namespace PowerShellDBDrive.Provider
{
    [CmdletProvider( "DatabaseProvider", ProviderCapabilities.None )]
    public class DatabaseProvider : NavigationCmdletProvider 
    {
		public const string PATH_SEPARATOR = "\\"; 
		/// <summary> 
		/// Defines the types of paths to items. 
		/// </summary> 
		private enum PathType 
		{ 
			/// <summary>
			/// Path to a schema.
			/// </summary>
			Schema,
			
			/// <summary>
			/// Path to a table item.
			/// </summary>
			Table,
			
			/// <summary>
			/// Path to a row item.
			/// </summary>
			Row,
			
			/// <summary>
			/// A path to an item that is not a database, table, or row.
			/// </summary>
			Invalid 
		}
		
		#region Drive Manipulation 
		
		/// <summary> 
		/// The Windows PowerShell engine calls this method when the New-Drive  
		/// cmdlet is run. This provider creates a connection to the database  
		/// file and sets the Connection property in the PSDriveInfo. 
		/// </summary> 
		/// <param name="drive"> 
		/// Information describing the drive to create. 
		/// </param> 
		/// <returns>An object that describes the new drive.</returns> 
        protected override PSDriveInfo NewDrive( PSDriveInfo drive )
        {
			// Check to see if the supplied drive object is null. 
			if (drive == null)
			{
				WriteError(new ErrorRecord(
									   new ArgumentNullException("drive"),
									   "NullDrive",
									   ErrorCategory.InvalidArgument,
									   null));
				return null;
			}
			
			if (String.IsNullOrEmpty(drive.Root)) {
				WriteError(new ErrorRecord(
									   new ArgumentNullException("drive.Root"),
									   "NullRoot",
									   ErrorCategory.InvalidArgument,
									   null));
				return null;
			}
			var driveParams = this.DynamicParameters as DatabaseParameters;
			var driveInfo = new DatabaseDriveInfo(drive, driveParams);
			var connection = new OleDbConnection(driveInfo.Root);
			connection.Open();
			driveInfo.DatabaseConnection = connection;
            return driveInfo;
        }
		
		/// <summary> 
		/// The Windows PowerShell engine calls this method when the  
		/// Remove-Drive cmdlet is run. 
		/// </summary> 
		/// <param name="drive">The drive to remove.</param> 
		/// <returns>The drive to be removed.</returns> 
		protected override PSDriveInfo RemoveDrive(PSDriveInfo drive) 
		{ 
			// Check to see if the supplied drive object is null. 
			if (drive == null) { 
				WriteError(new ErrorRecord( 
										new ArgumentNullException("drive"), 
										"NullDrive", 
										ErrorCategory.InvalidArgument, 
										drive));
				return null; 
			}
			
			var driveInfo = drive as DatabaseDriveInfo;
			if (driveInfo == null) {
				return null;
			}
			
			driveInfo.DatabaseConnection.Close();
			
			return driveInfo;
		}
		
        protected override object NewDriveDynamicParameters()
        {
            return new DatabaseParameters();
        }
		
		#endregion Drive Manipulation
		
	    /// <summary> 
		/// Test to see if the specified path is syntactically valid. 
		/// </summary> 
		/// <param name="path">The path to validate.</param> 
		/// <returns>True if the specified path is valid.</returns> 
		protected override bool IsValidPath(string path) 
		{ 
			// Check to see if the path is null or empty. 
			if (String.IsNullOrEmpty(path)) 
			{ 
				return false; 
			}
			path = NormalizePath(path);
			
			string[] pathElements = path.Split(PATH_SEPARATOR.ToCharArray());
			
			foreach (string element in pathElements) {
				if (element.Length == 0) {
					return false;
				}
			}
			
			return true;
		}
		
		/// <summary> 
		/// Adapts the path, making sure the correct path separator character is used. 
		/// </summary> 
		/// <param name="path">Path to normalize.</param> 
		/// <returns>Normalized path.</returns> 
		private string NormalizePath(string path) 
		{ 
			string result = path;
			
			if (!String.IsNullOrEmpty(path)) { 
				result = path.Replace("/", PATH_SEPARATOR); 
			} 

			return result; 
		}
    }
}