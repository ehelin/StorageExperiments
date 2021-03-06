﻿using System;
using Shared.dto.blob;
using Shared;
using Microsoft.WindowsAzure.Storage.Blob;
using Shared.dto.threading;
using Shared.dto;

namespace Blob
{
    public class BlobMain : Shared.dto.Main
    {
        private bool DeleteFiles = false;
        private bool DeleteContainers = false;

        public BlobMain() { }

        public BlobMain(string pAzureConnectionString,
                        string pAzureContainerName,
                        int pMaxThreads,
                        long sourceRecordTotal,
                        bool pDeleteAllFiles,
                        bool pDeleteContainers)
        {
            this.Credentials = new BlobDataStorageCredentials(pAzureConnectionString, pAzureContainerName);
            this.StorageType = Enumeration.StorageTypes.Blob;
            this.MaxThreadsAllowed = pMaxThreads;

            if (sourceRecordTotal > 0)
                this.TotalRecordCount = sourceRecordTotal;
            else
                SetRecordCountTotal();

            this.DeleteFiles = pDeleteAllFiles;
            this.DeleteContainers = pDeleteContainers;
        }

        public void Run()
        {
            Setup();
            RunExample();
        }

        public void RunQueries(string pAzureConnectionString,
                              string pAzureContainerName)
        {
            ThreadJob tj = new BlobLoadThreadJob();
            tj.RunCountQueries(new BlobDataStorageCredentials(pAzureConnectionString, pAzureContainerName));
        }

        #region Setup

        private void Setup()
        {
            if (this.DeleteFiles)
            {
                DeleteAllFiles();
            }
            else if (this.DeleteContainers)
            {
                DeleteAllContainers();
                System.Threading.Thread.Sleep(300000);                    //wait for all containers to be deleted (5 minutes)
            }

            //recreate blob container
            BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)Credentials;
            CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, true);
        }
        private void DeleteAllFiles()
        {
            BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)Credentials;
            CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, false);

            Console.WriteLine("Starting to delete blob files for cleanup...");

            if (container.Exists())
            {
                foreach (IListBlobItem item in container.ListBlobs(null, false))
                {
                    if (item.GetType() == typeof(CloudBlockBlob))
                    {
                        CloudBlockBlob blob = (CloudBlockBlob)item;
                        blob.Delete();
                    }
                }
            }

            Console.WriteLine("Done deleting blob files for cleanup!");
        }
        private void DeleteAllContainers()
        {
            BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)Credentials;
            CloudBlobClient client = Utilities.GetBlobStorageClient(bsc.azureConnectionString);

            Console.WriteLine("Starting to delete blob containers for cleanup...");

            foreach (CloudBlobContainer container in client.ListContainers())
                container.DeleteIfExists();

            Console.WriteLine("Done deleting blob containers for cleanup!");
        }

        #endregion
    }
}
