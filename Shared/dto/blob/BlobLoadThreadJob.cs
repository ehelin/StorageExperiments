using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;
using Shared.dto.source;
using Shared.dto.threading;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Linq;

namespace Shared.dto.blob
{
    public class BlobLoadThreadJob : ThreadJob
    {
        public BlobLoadThreadJob() : base() { }

        public BlobLoadThreadJob(DataStorageCredentials pCredentials,
                                 long recordCount,
                                 long startId,
                                 int threadId)
            : base(pCredentials, recordCount, startId, threadId)
        { }

        protected async override void RunLoad(int pThreadId, long pRecordCount, DataStorageCredentials pCredentials, long pStartId, long pEndPoint)
        {
            LoadRecords(pThreadId, pRecordCount, pCredentials, pStartId, pEndPoint);
        }

        private async void LoadRecords(int pThreadId, long pRecordCount, DataStorageCredentials pCredentials, long pStartId, long pEndPoint)
        {
            Database db = new Database();
            BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)Credentials;
            CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, false);
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
                        UploadBlob(sr, container);
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
        private void UploadBlob(SourceRecord sr, CloudBlobContainer container)
        {
            int errorTryCtr = 0;
            string blobName = "Thread" + this.threadId.ToString() + "_Db-" + sr.Id.ToString() + "_" + sr.Type.ToString();
            string data = Utilities.SerializeSourceRecord(sr);
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            try
            {
                MemoryStream stream = new MemoryStream();
                IFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, data);

                //Credit - https://social.msdn.microsoft.com/Forums/azure/en-US/a9f8dae4-5636-43d0-b177-e631d9c8d92c/blob-uploadfromstream-saves-empty-files?forum=windowsazuredata
                //I was getting empty blob files
                stream.Seek(0, SeekOrigin.Begin);

                blockBlob.UploadFromStream(stream);
            }
            catch (Exception e)
            {
                if (errorTryCtr > Constants.MAX_RETRY)
                    Console.WriteLine("Blob - Max error limit reached...moving on");
                else
                {
                    Console.WriteLine("Blob ERROR: - " + e.Message);
                    errorTryCtr++;
                }
            }
        }

        public override void RunCountQueries(DataStorageCredentials pCredentials)
        {
            GetTotalRecordCount(pCredentials);
            GetSpecificId();
            GetCountForSpecificType();
        }
        private void GetTotalRecordCount(DataStorageCredentials pCredentials)
        {
            long recordCnt = 0;
            BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)pCredentials;
            CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, false);

            Console.WriteLine("Starting total blob record count! " + DateTime.Now.ToString());

            foreach (IListBlobItem item in container.ListBlobs(null, false))
                recordCnt++;

            Console.WriteLine("There were " + recordCnt.ToString() + " Inserted! " + DateTime.Now.ToString());
        }
        private void GetSpecificId()
        {
            bool recordExists = false;
            BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)Credentials;
            CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, false);

            Console.WriteLine("Starting specific record search in blob storage for id " + this.TestRecordId.ToString() + " - " + DateTime.Now.ToString());

            foreach (IListBlobItem item in container.ListBlobs(null, false))
            {
                if (item.GetType() == typeof(CloudBlockBlob))
                {
                    CloudBlockBlob blob = (CloudBlockBlob)item;

                    //Thread1_Db-1000000_East_StatusUpdate_20164517064523768PM
                    int start = blob.Name.IndexOf("-");
                    string afterDb = blob.Name.Substring(start, blob.Name.Length - start);

                    int end = afterDb.IndexOf("_");
                    string dbIdStr = afterDb.Substring(0, end);
                    dbIdStr = dbIdStr.Replace("-", "");
                    dbIdStr = dbIdStr.Replace("_", "");

                    //better test...is the id stored in the blob name?
                    if (dbIdStr.Equals(this.TestRecordId.ToString()))
                    {
                        recordExists = true;
                        break;
                    }
                }
            }

            Console.WriteLine("Record exists (true/false): " + recordExists.ToString() + " - " + DateTime.Now.ToString());
        }
        private void GetCountForSpecificType()
        {
            long recordCnt = 0;
            BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)Credentials;
            CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, false);

            Console.WriteLine("Starting specific record search in blob storage for type " + this.TestType + " - " + DateTime.Now.ToString());

            foreach (IListBlobItem item in container.ListBlobs(null, false))
            {
                if (item.GetType() == typeof(CloudBlockBlob))
                {
                    CloudBlockBlob blob = (CloudBlockBlob)item;

                    if (blob.Name.IndexOf(this.TestType) != -1)
                        recordCnt++;
                }
            }

            Console.WriteLine("There were " + recordCnt.ToString() + " matching the " + this.TestType + " type! " + DateTime.Now.ToString());
        }
    }
}
