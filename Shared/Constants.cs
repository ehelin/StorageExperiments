namespace Shared
{
    public class SourceDataConstants
    {
        public const string DB_CONNECTION = "Data Source=;Initial Catalog=Satellite;Integrated Security=true;";
        public const int DB_TIMEOUT = 30000000;  //seconds

        public const string SQL_GET_RECORD_COUNT = "SELECT count(*) FROM [Satellite].[dbo].[Updates]";
        public const string SQL_GET_RECORD_ID = "select id, type, data, created from dbo.Updates where id = @id";

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
