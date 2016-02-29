
using System;
using System.ComponentModel;
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

        public BlobLoadThreadJob(ThreadCompletion pDone,
                                 DataStorageCredentials pCredentials,
                                 long recordCount,
                                 long startId,
                                 int threadId) : base(pDone, pCredentials, recordCount, startId, threadId)
        { }

        public override void DoWork(object sender, DoWorkEventArgs e)
        {
            Database db = new Database();
            BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)Credentials;
            CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, false);
            int ctr = 0;

            Console.WriteLine("Thread " + threadId + " starting with " + this.recordCount.ToString() + " records! " + DateTime.Now.ToString());

            while (ctr <= this.recordCount)
            {
                SourceRecord sr = db.GetRecord(startId);
                UploadBlob(sr, container);

                startId++;
                ctr++;
            }

            Console.WriteLine("Thread " + threadId + " Done! " + DateTime.Now.ToString());
        }

        private void UploadBlob(SourceRecord sr, CloudBlobContainer container)
        {
            string blobName = "Thread" + this.threadId.ToString() + "_Db-" + sr.Id.ToString() + "_" + sr.Type.ToString();

            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            MemoryStream stream = new MemoryStream();
            IFormatter formatter = new BinaryFormatter();
            formatter.Serialize(stream, sr.Data);

            blockBlob.UploadFromStream(stream);
        }
    }
}
