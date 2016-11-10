using System;
using System.Data.Common;

namespace PowerShellDBDrive.DataModel.Oracle
{
    /// <summary>
    /// Simple static class that contains all methods needed to build Oracle related objects.
    /// </summary>
    public static class OracleDatabaseFactory
    {
        public static OracleDatabaseViewInfo BuildDatabaseViewInfo(DbDataReader reader)
        {
            OracleDatabaseViewInfo dvi = new OracleDatabaseViewInfo();
            dvi.Owner = reader["OWNER"] as string;
            dvi.ViewName = reader["VIEW_NAME"] as string;
            dvi.TextLength = reader["TEXT_LENGTH"] as long?;
            dvi.Text = reader["TEXT"] as string;
            dvi.TypeTextLength = reader["TYPE_TEXT_LENGTH"] as long?;
            dvi.TypeText = reader["TYPE_TEXT"] as string;
            dvi.OidTextLength = reader["OID_TEXT_LENGTH"] as long?;
            dvi.OidText = reader["OID_TEXT"] as string;
            dvi.ViewTypeOwner = reader["VIEW_TYPE_OWNER"] as string;
            dvi.ViewType = reader["VIEW_TYPE"] as string;
            dvi.SuperviewName = reader["SUPERVIEW_NAME"] as string;
            return dvi;
        }

        public static OracleDatabaseColumnInfo BuildDatabaseColumnInfo(DbDataReader reader)
        {
            OracleDatabaseColumnInfo dci = new OracleDatabaseColumnInfo();
            dci.Owner = reader["OWNER"] as string;
            dci.TableName = reader["TABLE_NAME"] as string;
            dci.ColumnName = reader["COLUMN_NAME"] as string;
            dci.DataType = reader["DATA_TYPE"] as string;
            dci.DataTypeMod = reader["DATA_TYPE_MOD"] as string;
            dci.DataTypeOwner = reader["DATA_TYPE_OWNER"] as string;
            dci.DataLength = reader["DATA_LENGTH"] as long?;
            dci.DataPrecision = reader["DATA_PRECISION"] as long?;
            dci.DataScale = reader["DATA_SCALE"] as long?;
            dci.Nullable = reader["NULLABLE"] as string;
            dci.ColumnId = reader["COLUMN_ID"] as long?;
            dci.DefaultLength = reader["DEFAULT_LENGTH"] as long?;
            dci.DataDefault = reader["DATA_DEFAULT"] as long?;
            dci.NumDistinct = reader["NUM_DISTINCT"] as long?;
            dci.LowValue = reader["LOW_VALUE"] as string;
            dci.HighValue = reader["HIGH_VALUE"] as string;
            dci.Density = reader["DENSITY"] as long?;
            dci.NumNulls = reader["NUM_NULLS"] as long?;
            dci.NumBuckets = reader["NUM_BUCKETS"] as long?;
            dci.LastAnalyzed = reader["LAST_ANALYZED"] as DateTime?;
            dci.SampleSize = reader["SAMPLE_SIZE"] as long?;
            dci.CharacterSetName = reader["CHARACTER_SET_NAME"] as string;
            dci.CharColDeclLength = reader["CHAR_COL_DECL_LENGTH"] as long?;
            dci.GlobalStats = reader["GLOBAL_STATS"] as string;
            dci.UserStats = reader["USER_STATS"] as string;
            dci.AvgColLen = reader["AVG_COL_LEN"] as long?;
            dci.CharLength = reader["CHAR_LENGTH"] as long?;
            dci.CharUsed = reader["CHAR_USED"] as string;
            dci.V80FmtImage = reader["V80_FMT_IMAGE"] as string;
            dci.DataUpgraded = reader["DATA_UPGRADED"] as string;
            dci.Histogram = reader["HISTOGRAM"] as string;
            return dci;
        }

        public static OracleDatabaseTableInfo BuildDatabaseTableInfo(DbDataReader reader)
        {
            OracleDatabaseTableInfo dti = new OracleDatabaseTableInfo();
            dti.Owner = reader["OWNER"] as string;
            dti.TableName = reader["TABLE_NAME"] as string;
            dti.TablespaceName = reader["TABLESPACE_NAME"] as string;
            dti.ClusterName = reader["CLUSTER_NAME"] as string;
            dti.IotName = reader["IOT_NAME"] as string;
            dti.Status = reader["STATUS"] as string;
            dti.PctFree = reader["PCT_FREE"] as long?;
            dti.PctUsed = reader["PCT_USED"] as long?;
            dti.IniTrans = reader["INI_TRANS"] as long?;
            dti.MaxTrans = reader["MAX_TRANS"] as long?;
            dti.InitialExtent = reader["INITIAL_EXTENT"] as long?;
            dti.NextExtent = reader["NEXT_EXTENT"] as long?;
            dti.MinExtents = reader["MIN_EXTENTS"] as long?;
            dti.MaxExtents = reader["MAX_EXTENTS"] as long?;
            dti.PctIncrease = reader["PCT_INCREASE"] as long?;
            dti.Freelists = reader["FREELISTS"] as long?;
            dti.FreelistGroups = reader["FREELIST_GROUPS"] as long?;
            dti.Logging = reader["LOGGING"] as string;
            dti.BackedUp = reader["BACKED_UP"] as string;
            dti.NumRows = reader["NUM_ROWS"] as long?;
            dti.Blocks = reader["BLOCKS"] as long?;
            dti.EmptyBlocks = reader["EMPTY_BLOCKS"] as long?;
            dti.AvgSpace = reader["AVG_SPACE"] as long?;
            dti.ChainCnt = reader["CHAIN_CNT"] as long?;
            dti.AvgRowLen = reader["AVG_ROW_LEN"] as long?;
            dti.AvgSpaceFreelistBlocks = reader["AVG_SPACE_FREELIST_BLOCKS"] as long?;
            dti.NumFreelistBlocks = reader["NUM_FREELIST_BLOCKS"] as long?;
            dti.Degree = reader["DEGREE"] as string;
            dti.Instances = reader["INSTANCES"] as string;
            dti.Cache = reader["CACHE"] as string;
            dti.TableLock = reader["TABLE_LOCK"] as string;
            dti.SampleSize = reader["SAMPLE_SIZE"] as long?;
            dti.LastAnalyzed = reader["LAST_ANALYZED"] as DateTime?;
            dti.Partitioned = reader["PARTITIONED"] as string;
            dti.IotType = reader["IOT_TYPE"] as string;
            dti.Temporary = reader["TEMPORARY"] as string;
            dti.Secondary = reader["SECONDARY"] as string;
            dti.Nested = reader["NESTED"] as string;
            dti.BufferPool = reader["BUFFER_POOL"] as string;
            dti.FlashCache = reader["FLASH_CACHE"] as string;
            dti.CellFlashCache = reader["CELL_FLASH_CACHE"] as string;
            dti.RowMovement = reader["ROW_MOVEMENT"] as string;
            dti.GlobalStats = reader["GLOBAL_STATS"] as string;
            dti.UserStats = reader["USER_STATS"] as string;
            dti.Duration = reader["DURATION"] as string;
            dti.SkipCorrupt = reader["SKIP_CORRUPT"] as string;
            dti.Monitoring = reader["MONITORING"] as string;
            dti.ClusterOwner = reader["CLUSTER_OWNER"] as string;
            dti.Dependencies = reader["DEPENDENCIES"] as string;
            dti.Compression = reader["COMPRESSION"] as string;
            dti.CompressFor = reader["COMPRESS_FOR"] as string;
            dti.Dropped = reader["DROPPED"] as string;
            dti.ReadOnly = reader["READ_ONLY"] as string;
            dti.SegmentCreated = reader["SEGMENT_CREATED"] as string;
            dti.ResultCache = reader["RESULT_CACHE"] as string;
            return dti;
        }
    }
}
