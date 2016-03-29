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

        protected override void RunCountQueries(DataStorageCredentials pCredentials)
        {
            throw new NotImplementedException();
        }
        private void GetRecordCount(DataStorageCredentials pCredentials)
        {
            TableStorageDataStorageCredentials tsc = (TableStorageDataStorageCredentials)Credentials;
            CloudTable table = Utilities.GetTableStorageContainer(false, tsc.azureConnectionString, tsc.azureContainerName);
            List<long> updates = null;

            try
            {
                updates = (from update in table.CreateQuery<SourceRecordTableStorage>()
                           select update.Id).ToList<long>();
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: TableStorageLoadThreadJob.cs->GetRecordCount(args): " + e.Message);
            }

            Console.WriteLine("There are " + updates.Count() + " records!");
        }
    }
}
