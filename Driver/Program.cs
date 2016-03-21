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
            //RunBlob();
            //RunTableStorage();
            //RunAzureSqlServerDb();
            //RunEventHub();
            //RunDocumentDb();
            //RunSimpleThreadAsyncAwait();
        }

        private static void RunBlob()
        {
            Console.WriteLine("Starting Blob " + DateTime.Now.ToString());

            string azConnection = "";
            string azContainterName = "datablob";

            BlobMain m = new BlobMain(azConnection, azContainterName, 32, 23310144, true, false);
            m.Run();

            Console.Read();  //hold open application

            Console.WriteLine("Blob Done! " + DateTime.Now.ToString());
        }
        private static void RunTableStorage()
        {
            Console.WriteLine("Starting TableStorage " + DateTime.Now.ToString());

            string azConnection = "";
            string azContainterName = "dataTableStorage";

            TableStorageMain tsm = new TableStorageMain(azConnection, azContainterName, 32, 23310144, true, false);
            tsm.Run();

            Console.Read();  //hold open application

            Console.WriteLine("TableStorage Done! " + DateTime.Now.ToString());
        }
        private static void RunAzureSqlServerDb()
        {
            Console.WriteLine("Starting SQL Server " + DateTime.Now.ToString());

            string dbConnection = "";
            SqlServerDbMain ssd = new SqlServerDbMain(dbConnection, 32, 23310144);
            ssd.Run();

            Console.Read();  //hold open application

            Console.WriteLine("SQL Server Done! " + DateTime.Now.ToString());
        }
        private static async void RunDocumentDb()
        {
            Console.WriteLine("Starting Document Db " + DateTime.Now.ToString());

            string EndpointUrl = "";
            string AuthorizationKey = "";

            DocumentDatabaseMain ddm = new DocumentDatabaseMain(EndpointUrl, AuthorizationKey, 5, 23310144, true, false);
            ddm.Run();

            Console.Read();  //hold open application

            Console.WriteLine("Document Db Done! " + DateTime.Now.ToString());
        }
        private static async void RunEventHub()
        {
            Console.WriteLine("Starting Event Hub " + DateTime.Now.ToString());

            string eventHub = "";
            string eventHubName = "satellitehub";

            EventHub.EventHub eh = new EventHub.EventHub(eventHub, eventHubName, 32, 23310144);
            eh.Run();

            Console.Read();  //hold open application

            Console.WriteLine("Event Hub Done! " + DateTime.Now.ToString());
        }

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
