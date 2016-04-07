using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Table;
using Shared.dto.source;
using Shared.dto.threading;
using System.Linq;

namespace Shared.dto.tablestorage
{
    public class TableStorageLoadThreadJob : ThreadJob
    {
        public TableStorageLoadThreadJob() : base() { }

        public TableStorageLoadThreadJob(DataStorageCredentials pCredentials,
                                 long recordCount,
                                 long startId,
                                 int threadId) : base(pCredentials, recordCount, startId, threadId)
        { }


        protected async override void RunLoad(int pThreadId, long pRecordCount, DataStorageCredentials pCredentials, long pStartId, long pEndPoint)
        {
            LoadRecords(pThreadId, pRecordCount, pCredentials, pStartId, pEndPoint);
        }

        private async void LoadRecords(int pThreadId, long pRecordCount, DataStorageCredentials pCredentials, long pStartId, long pEndPoint)
        {
            Database db = new Database();
            TableStorageDataStorageCredentials tsdsc = (TableStorageDataStorageCredentials)Credentials;
            CloudTable table = Utilities.GetTableStorageContainer(true, tsdsc.azureConnectionString, tsdsc.azureContainerName);
            long threadId = pThreadId;
            long recordCount = pRecordCount;
            long startId = pStartId;
            long endId = pEndPoint;

            Console.WriteLine("Thread " + threadId + " (RcrdCnt/Start/End) (" + recordCount.ToString() + "/"
                                + startId.ToString() + "/" + endId.ToString() + ")" + DateTime.Now.ToString());

            while (startId <= endId)
            {
                try
                {
                    IList<SourceRecord> scrRecords = db.GetRecord(startId, endId);

                    foreach (SourceRecord sr in scrRecords)
                    {
                        SourceRecordTableStorage srts = new SourceRecordTableStorage();
                        srts.Id = sr.Id;
                        srts.Type = sr.Type;
                        srts.Created = sr.Created;
                        srts.Data = sr.Data;
                        srts.SetParitionKeyRowKey();

                        //TestComplete.updates.Add(sr);
                        InsertRecord(srts, table);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }

                startId = startId + Convert.ToInt64(SourceDataConstants.SQL_GET_TOP_VALUE);
            }

            Console.WriteLine("Thread " + threadId + " Done! " + DateTime.Now.ToString());
        }
        private void InsertRecord(SourceRecordTableStorage srts, CloudTable table)
        {
            int errorTryCtr = 0;

            try
            {
                TableOperation insertOperation = TableOperation.Insert(srts);
                TableResult tr = table.Execute(insertOperation);
            }
            catch (Exception e)
            {
                if (errorTryCtr > Constants.MAX_RETRY)
                    Console.WriteLine("TableStorageLoad - Max error limit reached...moving on");
                else
                {
                    Console.WriteLine("TableStorageLoad ERROR: - " + e.Message);
                    errorTryCtr++;
                }
            }
        }

        public override void RunCountQueries(DataStorageCredentials pCredentials)
        {
            GetTotalRecordCount(pCredentials);
            GetSpecificId(pCredentials);
            GetCountForSpecificType(pCredentials);
        }

        private void GetTotalRecordCount(DataStorageCredentials pCredentials)
        {
            TableStorageDataStorageCredentials tsc = (TableStorageDataStorageCredentials)pCredentials;
            CloudTable table = Utilities.GetTableStorageContainer(false, tsc.azureConnectionString, tsc.azureContainerName);
            List<long> updates = null;

            Console.WriteLine("Starting total table storage record count! " + DateTime.Now.ToString());

            try
            {
                updates = (from update in table.CreateQuery<SourceRecordTableStorage>()
                           select update.Id).ToList<long>();
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: " + e.Message);
            }

            Console.WriteLine("There are " + updates.Count() + " records! " + DateTime.Now.ToString());
        }
        private void GetSpecificId(DataStorageCredentials pCredentials)
        {
            bool recordExists = false;
            TableStorageDataStorageCredentials tsc = (TableStorageDataStorageCredentials)pCredentials;
            CloudTable table = Utilities.GetTableStorageContainer(false, tsc.azureConnectionString, tsc.azureContainerName);
            List<long> updates = null;

            Console.WriteLine("Starting specific record search in table storage for id " + this.TestRecordId.ToString() + " - " + DateTime.Now.ToString());

            try
            {
                updates = (from update in table.CreateQuery<SourceRecordTableStorage>()
                           where update.Id.Equals(this.TestRecordId)
                           select update.Id).ToList<long>();

                if (updates.Count() == 1)
                    recordExists = true;
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: " + e.Message);
            }

            Console.WriteLine("Record exists (true/false): " + recordExists.ToString() + " - " + DateTime.Now.ToString());
        }
        private void GetCountForSpecificType(DataStorageCredentials pCredentials)
        {
            TableStorageDataStorageCredentials tsc = (TableStorageDataStorageCredentials)pCredentials;
            CloudTable table = Utilities.GetTableStorageContainer(false, tsc.azureConnectionString, tsc.azureContainerName);
            List<long> updates = null;

            Console.WriteLine("Starting specific record search in table storage for type " + this.TestType + " - " + DateTime.Now.ToString());

            try
            {
                //Fails****
                updates = (from update in table.CreateQuery<SourceRecordTableStorage>()
                           where update.Type == this.TestType
                           select update.Id).ToList<long>();

                //TODO - find alternative way to search for type...indexof throw 'not implemented' error
                //updates = (from update in table.CreateQuery<SourceRecordTableStorage>()
                //           where update.Type.IndexOf(this.TestType) != -1
                //           select update.Id).ToList<long>();
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: " + e.Message);
            }

            Console.WriteLine("There were " + updates.Count.ToString() + " matching the " + this.TestType + " type! " + DateTime.Now.ToString());
        }
    }
}
