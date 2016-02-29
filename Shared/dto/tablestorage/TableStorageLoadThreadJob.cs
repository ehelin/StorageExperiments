using System;
using System.ComponentModel;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Shared.dto.source;
using Shared.dto.threading;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace Shared.dto.tablestorage
{
    public class TableStorageLoadThreadJob : ThreadJob
    {
        public TableStorageLoadThreadJob() : base() { }

        public TableStorageLoadThreadJob(ThreadCompletion pDone,
                                 DataStorageCredentials pCredentials,
                                 long recordCount,
                                 long startId,
                                 int threadId) : base(pDone, pCredentials, recordCount, startId, threadId)
        { }

        public override void DoWork(object sender, DoWorkEventArgs e)
        {
            Database db = new Database();
            TableStorageDataStorageCredentials tsdsc = (TableStorageDataStorageCredentials)Credentials;
            CloudTable table = Utilities.GetTableStorageContainer(true, tsdsc.azureConnectionString, tsdsc.azureContainerName);
            int ctr = 0;

            Console.WriteLine("Thread " + threadId + " starting with " + this.recordCount.ToString() + " records! " + DateTime.Now.ToString());

            while (ctr <= this.recordCount)
            {
                SourceRecord sr = db.GetRecord(startId);
                SourceRecordTableStorage srts = new SourceRecordTableStorage();
                srts.Id = sr.Id;
                srts.Type = sr.Type;
                srts.Created = sr.Created;
                srts.Data = sr.Data;
                srts.SetParitionKeyRowKey();

                //TestComplete.updates.Add(sr);
                TableOperation insertOperation = TableOperation.Insert(srts);
                TableResult tr = table.Execute(insertOperation);

                startId++;
                ctr++;
            }

            Console.WriteLine("Thread " + threadId + " Done! " + DateTime.Now.ToString());
        }
    }
}
