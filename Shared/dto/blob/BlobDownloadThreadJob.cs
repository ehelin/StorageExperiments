using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Collections.Generic;
using System;

namespace Shared.dto.blob
{
    public class BlobDownloadThreadJob : Shared.dto.threading.DownloadThreadJob
    {
        public BlobDownloadThreadJob() { }

        public BlobDownloadThreadJob(DataStorageCredentials pCredentials, List<string> fileListPath) : base(pCredentials, fileListPath) 
        {
            this.credentials = pCredentials;
            this.fileListPath = fileListPath;
        }

        protected async override void Download(DataStorageCredentials credentials, int threadId, string fileListPath)
        {
            DownloadItems(credentials, threadId, fileListPath);
        }
        protected async void DownloadItems(DataStorageCredentials credentials, int threadId, string fileListPath)
        {
            BlobDataStorageCredentials blobCred = (BlobDataStorageCredentials)credentials;
            CloudBlobContainer srcContainer = Shared.Utilities.GetBlobStorageContainer(blobCred.azureConnectionString, blobCred.azureContainerName, false);
            FileInfo fi = new FileInfo(fileListPath);
            string localPath = fi.DirectoryName;
            StreamReader sr = null;

            Console.WriteLine("Download Thread " + threadId.ToString() + " starting! " + DateTime.Now.ToString());

            try
            {
                sr = new StreamReader(fileListPath);

                while (sr.Peek() != -1)
                {
                    string file = sr.ReadLine();
                    CloudBlockBlob blockBlob = srcContainer.GetBlockBlobReference(file);
                    DownloadFile(blockBlob, localPath);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ThreadId: " + threadId.ToString() + ", Error: " + e.Message);
            }
            finally
            {
                Utilities.CloseIoObjects(sr, null);
            }

            Console.WriteLine("Download Thread " + threadId.ToString() + " Complete! " + DateTime.Now.ToString());
        }

        private void DownloadFile(CloudBlockBlob cbb, string localDestPath)
        {
            string downloadPath = localDestPath + "\\" + cbb.Name + "_downloaded.json";

            if (File.Exists(downloadPath))
            {
                File.Delete(downloadPath);
            }

            using (var fileStream = System.IO.File.OpenWrite(downloadPath))
            {
                cbb.DownloadToStream(fileStream);
            }
        }
    }
}
