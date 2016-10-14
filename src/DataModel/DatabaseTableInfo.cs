using System;
using System.Data;

namespace PowerShellDBDrive {
	
	public interface IDatabaseTableInfo {
		string SchemaName {get;}
		string TableName {get;}
		long RowCount {get;}
	}
	
	/// <summary> 
	/// Contains information specific to the database table. 
	/// Similar to the DirectoryInfo class. 
	/// </summary> 
	public class DatabaseTableInfo : IDatabaseTableInfo
	{
		public string Owner                     {get; set; }
		public string TableName                 {get; set; }
		public string TablespaceName            {get; set; }        
		public string ClusterName               {get; set; }        
		public string IotName                   {get; set; }         
		public string Status                    {get; set; }          
		public long? PctFree                   {get; set; }         
		public long? PctUsed                   {get; set; }         
		public long? IniTrans                  {get; set; }         
		public long? MaxTrans                  {get; set; }         
		public long? InitialExtent             {get; set; }         
		public long? NextExtent                {get; set; }         
		public long? MinExtents                {get; set; }         
		public long? MaxExtents                {get; set; }         
		public long? PctIncrease               {get; set; }         
		public long? Freelists                 {get; set; }          
		public long? FreelistGroups            {get; set; }         
		public string Logging                   {get; set; }          
		public string BackedUp                  {get; set; }         
		public long? NumRows                   {get; set; }         
		public long? Blocks                    {get; set; }          
		public long? EmptyBlocks               {get; set; }         
		public long? AvgSpace                  {get; set; }         
		public long? ChainCnt                  {get; set; }         
		public long? AvgRowLen                 {get; set; }        
		public long? AvgSpaceFreelistBlocks    {get; set; }       
		public long? NumFreelistBlocks         {get; set; }        
		public string Degree                    {get; set; }          
		public string Instances                 {get; set; }          
		public string Cache                     {get; set; }          
		public string TableLock                 {get; set; }         
		public long? SampleSize                {get; set; }         
		public DateTime? LastAnalyzed              {get; set; }         
		public string Partitioned               {get; set; }          
		public string IotType                   {get; set; }         
		public string Temporary                 {get; set; }          
		public string Secondary                 {get; set; }          
		public string Nested                    {get; set; }          
		public string BufferPool                {get; set; }         
		public string FlashCache                {get; set; }         
		public string CellFlashCache            {get; set; }        
		public string RowMovement               {get; set; }         
		public string GlobalStats               {get; set; }         
		public string UserStats                 {get; set; }         
		public string Duration                  {get; set; }          
		public string SkipCorrupt               {get; set; }         
		public string Monitoring                {get; set; }          
		public string ClusterOwner              {get; set; }         
		public string Dependencies              {get; set; }          
		public string Compression               {get; set; }          
		public string CompressFor               {get; set; }         
		public string Dropped                   {get; set; }          
		public string ReadOnly                  {get; set; }         
		public string SegmentCreated            {get; set; }         
		public string ResultCache               {get; set; }
		
		/// <summary> 
		/// Definitions of columns.
		/// </summary> 
		public DatabaseColumnInfo[] Columns { get; set; }

		/// <summary> 
		/// Initializes a new instance of the DatabaseTableInfo class. 
		/// </summary> 
		/// <param name="row">The row definition.</param> 
		/// <param name="name">The table name.</param> 
		/// <param name="rowCount">The number of rows in the table.</param> 
		/// <param name="columns">Information on the column tables.</param> 
		public DatabaseTableInfo() { 
			
		}
		
		#region Interface Methods
		
		public string SchemaName { 
			get {
				return Owner;
			} 
		}
		
		public long RowCount {
			get {
				return NumRows.HasValue ? NumRows.Value : 0;
			}
		}
		
		#endregion Interface Methods
	}
}