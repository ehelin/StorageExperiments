using System;
using Shared.dto.threading;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using Shared.dto.tablestorage;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Table;

namespace Shared.dto.blob
{
    public class TableStorageTestCompletion : TestComplete
    {
        public override void HandleTestComplete(DataStorageCredentials Credentials)
        {
            //TestComplete.RunUpdateCount();

            TableStorageDataStorageCredentials tsc = (TableStorageDataStorageCredentials)Credentials;
            CloudTable table = Utilities.GetTableStorageContainer(false, tsc.azureConnectionString, tsc.azureContainerName);
            
            List<SourceRecordTableStorage> updates = (from update in table.CreateQuery<SourceRecordTableStorage>()
                                                      select update).ToList<SourceRecordTableStorage>();

            Console.WriteLine("There are " + updates.Count() + " records!");
        }
    }
}
