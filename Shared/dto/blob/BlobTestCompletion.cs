using System;
using Shared.dto.threading;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Shared.dto.blob
{
    public class BlobTestCompletion : TestComplete
    {
        public override void HandleTestComplete(DataStorageCredentials Credentials)
        {
            //TestComplete.RunUpdateCount();

            BlobDataStorageCredentials bsc = (BlobDataStorageCredentials)Credentials;
            CloudBlobContainer container = Utilities.GetBlobStorageContainer(bsc.azureConnectionString, bsc.azureContainerName, false);
            int count = 0;

            foreach (IListBlobItem item in container.ListBlobs(null, false))
                count++;

            Console.WriteLine("There are " + count.ToString() + " records!");
        }
    }
}
