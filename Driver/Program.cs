using System;
using Blob;
using TableStorage;
using SqlServerDb;
using DocumentDatabase;
using EventHub;

namespace Driver
{
    class Program
    {
        static void Main(string[] args)
        {
            //RunBlob();
            //RunTableStorage();
            //RunAzureSqlServerDb();
            //RunDocumentDb();
            RunEventHub();
        }

        private static void RunBlob()
        {
            Console.WriteLine("Starting Blob " + DateTime.Now.ToString());

            string azConnection = "";
            string azContainterName = "dataBlob";

            BlobMain m = new BlobMain(azConnection, azContainterName, 32, 23310144, false, true);
            m.Run();

            Console.WriteLine("Blob Done! " + DateTime.Now.ToString());
        }
        private static void RunTableStorage()
        {
            Console.WriteLine("Starting TableStorage " + DateTime.Now.ToString());

            string azConnection = "";
            string azContainterName = "dataTableStorage";

            TableStorageMain tsm = new TableStorageMain(azConnection, azContainterName, 32, 23310144, true, false);
            tsm.Run();

            Console.WriteLine("TableStorage Done! " + DateTime.Now.ToString());
        }
        private static void RunAzureSqlServerDb()
        {
            Console.WriteLine("Starting SQL Server " + DateTime.Now.ToString());

            string dbConnection = "";
            SqlServerDbMain ssd = new SqlServerDbMain(dbConnection, 32, 23310144);
            ssd.Run();

            Console.WriteLine("SQL Server Done! " + DateTime.Now.ToString());
        }
        private static async void RunDocumentDb()
        {
            Console.WriteLine("Starting Document Db " + DateTime.Now.ToString());

            string EndpointUrl = "";
            string AuthorizationKey = "";

            DocumentDatabaseMain ddm = new DocumentDatabaseMain(EndpointUrl, AuthorizationKey, 1, 23310144, true, true);
            ddm.Run();

            Console.WriteLine("Document Db Done! " + DateTime.Now.ToString());
        }
        private static async void RunEventHub()
        {
            Console.WriteLine("Starting Event Hub " + DateTime.Now.ToString());

            string eventHub = "";
            string eventHubName = "satellitehub";

            EventHub.EventHub eh = new EventHub.EventHub(eventHub, eventHubName, 1, 100);
            eh.Run();

            Console.WriteLine("Event Hub Done! " + DateTime.Now.ToString());
        }
    }
}
