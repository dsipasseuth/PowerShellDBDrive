using System;

namespace PowerShellDBDrive.DataModel.Oracle
{

    /// <summary> 
    /// Contains information specific to the database table column.
    /// </summary> 
    public class OracleDatabaseColumnInfo : IDatabaseColumnInfo
    {
        public string Owner { get; set; }
        public string TableName { get; set; }
        public string ColumnName { get; set; }
        public string DataType { get; set; }
        public string DataTypeMod { get; set; }
        public string DataTypeOwner { get; set; }
        public long? DataLength { get; set; }
        public long? DataPrecision { get; set; }
        public long? DataScale { get; set; }
        public string Nullable { get; set; }
        public long? ColumnId { get; set; }
        public long? DefaultLength { get; set; }
        public long? DataDefault { get; set; }
        public long? NumDistinct { get; set; }
        public string LowValue { get; set; }
        public string HighValue { get; set; }
        public long? Density { get; set; }
        public long? NumNulls { get; set; }
        public long? NumBuckets { get; set; }
        public DateTime? LastAnalyzed { get; set; }
        public long? SampleSize { get; set; }
        public string CharacterSetName { get; set; }
        public long? CharColDeclLength { get; set; }
        public string GlobalStats { get; set; }
        public string UserStats { get; set; }
        public long? AvgColLen { get; set; }
        public long? CharLength { get; set; }
        public string CharUsed { get; set; }
        public string V80FmtImage { get; set; }
        public string DataUpgraded { get; set; }
        public string Histogram { get; set; }

        public OracleDatabaseColumnInfo()
        {

        }

        #region Interface Methods

        public string SchemaName
        {
            get
            {
                return Owner;
            }
        }

        public bool Nillable
        {
            get
            {
                return String.Equals(Nullable, "N");
            }
        }

        #endregion Interface Methods
    }
}