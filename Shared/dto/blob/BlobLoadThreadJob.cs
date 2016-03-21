using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;
using Shared.dto.source;
using Shared.dto.threading;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace Shared.dto.blob
{
    public class BlobLoadThreadJob : ThreadJob
    {
        public BlobLoadThreadJob() : base() { }

        public BlobLoadThreadJob(DataStorageCredentials pCredentials,
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
            BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)Credentials;
            CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, false);
            long threadId = pThreadId;
            long recordCount = pRecordCount;
            long startId = pStartId;
            long endId = pEndPoint;

            Console.WriteLine("Thread " + threadId + " starting with " + recordCount.ToString() + " records! " + DateTime.Now.ToString());

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
        protected override void GetRecordCount(DataStorageCredentials pCredentials)
        {
            long recordCnt = 0;
            BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)Credentials;
            CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, false);

            foreach (IListBlobItem item in container.ListBlobs(null, false))
                recordCnt++;

            Console.WriteLine("There were " + recordCnt.ToString() + " Inserted! " + DateTime.Now.ToString());
        }
    }
}
