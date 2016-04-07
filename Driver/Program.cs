using System;
using Blob;
using TableStorage;
using SqlServerDb;
using DocumentDatabase;
using Shared;

namespace Driver
{
    class Program
    {
        static void Main(string[] args)
        {
            //RunDataLoads();
            //RunSimpleThreadAsyncAwait();
            RunQueries();
        }

        #region Queries

        //NOTE:  Each method can be run after the other
        private static void RunQueries()
        {
            RunEventHubQueries(); 
            RunAzureSqlServerDbQueries();
            RunBlobQueries();
            RunTableStorageQueries();
            //RunDocumentDbQueries();

            Console.Read();  //hold open application
        }

        private static void RunBlobQueries()
        {
            string azConnection = "";
            string azContainterName = "datablob";

            BlobMain m = new BlobMain();
            m.RunQueries(azConnection, azContainterName);
        }
        private static void RunTableStorageQueries()
        {
            string azConnection = "";
            string azContainterName = "dataTableStorage";

            TableStorageMain tsm = new TableStorageMain();
            tsm.RunQueries(azConnection, azContainterName);
        }
        private static void RunAzureSqlServerDbQueries()
        {
            Console.WriteLine("Starting SQL Server Data Load " + DateTime.Now.ToString());

            string dbConnection = "";
            SqlServerDbMain ssd = new SqlServerDbMain();
            ssd.RunQueries(dbConnection);
        }
        private static async void RunDocumentDbQueries()
        {
            string EndpointUrl = "";
            string AuthorizationKey = "";

            DocumentDatabaseMain ddm = new DocumentDatabaseMain();
            ddm.RunQueries(EndpointUrl, AuthorizationKey);
        }
        private static async void RunEventHubQueries()
        {
            string dbConnection = "";
            EventHub.EventHub eh = new EventHub.EventHub(dbConnection);
            eh.RunQueries();
        }

        #endregion

        #region Data Loads

        //NOTE:  Each data load is meant to be run seperately from the others.  Each load is multi-threaded
        private static void RunDataLoads()
        {
            RunBlob();
            //RunTableStorage();
            //RunAzureSqlServerDb();
            //RunEventHub();
            //RunDocumentDb();
        }

        private static void RunBlob()
        {
            Console.WriteLine("Starting Blob Data Load " + DateTime.Now.ToString());

            string azConnection = "";
            string azContainterName = "datablob";

            BlobMain m = new BlobMain(azConnection, azContainterName, 32, 23310144, true, false);
            m.Run();

            Console.Read();  //hold open application

            Console.WriteLine("Blob Done! " + DateTime.Now.ToString());
        }
        private static void RunTableStorage()
        {
            Console.WriteLine("Starting TableStorage Data Load " + DateTime.Now.ToString());

            string azConnection = "";
            string azContainterName = "dataTableStorage";

            TableStorageMain tsm = new TableStorageMain(azConnection, azContainterName, 32, 23310144, true, false);
            tsm.Run();

            Console.Read();  //hold open application

            Console.WriteLine("TableStorage Done! " + DateTime.Now.ToString());
        }
        private static void RunAzureSqlServerDb()
        {
            Console.WriteLine("Starting SQL Server Data Load " + DateTime.Now.ToString());

            string dbConnection = "";
            SqlServerDbMain ssd = new SqlServerDbMain(dbConnection, 32, 23310144);
            ssd.Run();

            Console.Read();  //hold open application

            Console.WriteLine("SQL Server Done! " + DateTime.Now.ToString());
        }
        private static async void RunDocumentDb()
        {
            Console.WriteLine("Starting Document Db Data Load " + DateTime.Now.ToString());

            string EndpointUrl = "";
            string AuthorizationKey = "";

            DocumentDatabaseMain ddm = new DocumentDatabaseMain(EndpointUrl, AuthorizationKey, 5, 23310144, true, false);
            ddm.Run();

            Console.Read();  //hold open application

            Console.WriteLine("Document Db Done! " + DateTime.Now.ToString());
        }
        private static async void RunEventHub()
        {
            Console.WriteLine("Starting Event Hub Data Load " + DateTime.Now.ToString());

            string eventHub = "";
            string eventHubName = "satellitehub";

            EventHub.EventHub eh = new EventHub.EventHub(eventHub, eventHubName, 32, 23310144);
            eh.Run();

            Console.Read();  //hold open application

            Console.WriteLine("Event Hub Done! " + DateTime.Now.ToString());
        }

        #endregion

        #region Simple Async Await Sample

        private static SimpleThreadAsyncAwaitExample staae = null;

        private static void RunSimpleThreadAsyncAwait()
        {
            staae = new SimpleThreadAsyncAwaitExample();

            WaitVersionSingleThread();
            NoWaitVersionSingleThread();
            NoWaitVersionMultipleThreads();
            NoWaitVersionMultipleThreadsTaskWhenAll();

            Console.Read();    //wait for developer to close application
        }

        private static void WaitVersionSingleThread()
        {
            Console.WriteLine("Calling GetStringAndWait()...");

            string waitResult = staae.GetStringAndWait().Result;
            WaitLoop();

            Console.WriteLine("Thread completion value: " + SimpleThreadAsyncAwaitExample.clsValue);
            Console.WriteLine("Thread returned value: " + waitResult);
        }
        private static void NoWaitVersionSingleThread()
        {
            Console.WriteLine("Calling GetStringAndNoWait()...");

            Console.WriteLine("Starting thread...");

            SimpleThreadAsyncAwaitExample.clsValue = string.Empty;
            staae.GetStringAndNoWait(false, false);
            WaitLoop();

            Console.WriteLine("Thread completion value: " + SimpleThreadAsyncAwaitExample.clsValue);
        }
        private static void NoWaitVersionMultipleThreads()
        {
            Console.WriteLine("Calling GetStringAndNoWait()...");

            Console.WriteLine("Starting thread...");

            SimpleThreadAsyncAwaitExample.clsValue = string.Empty;
            staae.GetStringAndNoWait(true, false);

            WaitLoop();

            Console.WriteLine("Thread completion value: " + SimpleThreadAsyncAwaitExample.clsValue);
        }
        private static void NoWaitVersionMultipleThreadsTaskWhenAll()
        {
            Console.WriteLine("Calling GetStringAndNoWait()...");

            Console.WriteLine("Starting thread...");

            SimpleThreadAsyncAwaitExample.clsValue = string.Empty;
            staae.GetStringAndNoWait(true, true);

            Console.WriteLine("Thread completion value: " + SimpleThreadAsyncAwaitExample.clsValue);
        }
        private static void WaitLoop()
        {
            int waitingCtr = 1;

            while (string.IsNullOrEmpty(SimpleThreadAsyncAwaitExample.clsValue))
            {
                Console.WriteLine("Waiting Counter: " + waitingCtr.ToString());
                System.Threading.Thread.Sleep(1000);
                waitingCtr++;
            }
        }

        #endregion
    }
}
