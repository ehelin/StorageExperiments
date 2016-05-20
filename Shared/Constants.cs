namespace Shared
{
    public class Constants
    {
        public const int MAX_RETRY = 5;

        public const string DYNAMO_DB_TABLE_NAME = "SatelliteUpdates";
    }

    public class SourceDataConstants
    {
        public const string DB_CONNECTION = "Data Source=;Initial Catalog=Satellite;Integrated Security=true;";
        public const int DB_TIMEOUT = 30000000;  //seconds

        public const string SQL_GET_TOP_VALUE = "10000";
        public const string SQL_GET_RECORD_COUNT = "SELECT count(*) FROM [Satellite].[dbo].[Updates]";
        public const string SQL_GET_RECORD_ID = "select top " + SourceDataConstants.SQL_GET_TOP_VALUE + " id, type, data, created from dbo.Updates where id >= @startId and id <= @endId order by id ";

        public const string TYPE_CLIENT_UPDATE_INDEX_OF_SRC_TERM = "ClientUpdate";
        public const string TYPE_STATUS_UPDATE_INDEX_OF_SRC_TERM = "StatusUpdate";
    }

    public class DocumentDbConstants
    {
        public const string DOCUMENT_DB_NAME = "SatelliteDb";
        public const string DOCUMENT_DB_COLLECTION_NAME = "SatelliteDbCollection";
    }

    public class Exceptions
    {
        public const string ERR00000001 = "Unknown source update type";
        public const string ERR00000002 = "Document Db Database or document collection is not set";
    }
}
