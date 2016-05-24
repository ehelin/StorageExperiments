using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;
using Shared.dto.source;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Linq;

namespace Shared.dto.blob
{
    public class BlobDownloadThreadJob 
    {
        private DataStorageCredentials credentials;
        private int threadId;
        private string fileListPath;
        
        public BlobDownloadThreadJob(DataStorageCredentials credentials,
                                     int threadId, 
                                     string fileListPath)
        {
            this.credentials = credentials;
            this.threadId = threadId;
            this.fileListPath = fileListPath;
        }

        protected async Task<bool> Download()
        {
            BlobDataStorageCredentials blobCred = (BlobDataStorageCredentials)credentials;
            CloudBlobContainer srcContainer = Shared.Utilities.GetBlobStorageContainer(blobCred.azureConnectionString, blobCred.azureContainerName, false);

            bool goodDownload = false;

            StreamReader sr = new StreamReader(this.fileListPath);

            while (sr.Peek() != -1)
            {
                throw new NotImplementedException();
            }

            goodDownload = true;

            return goodDownload;
        }

        private void DownloadFile(CloudBlockBlob cbb, string localDestPath)
        {
            string downloadPath = localDestPath + cbb.Name + "_downloaded.json";

            using (var fileStream = System.IO.File.OpenWrite(downloadPath))
            {
                cbb.DownloadToStream(fileStream);
            }
        }
    }
}
