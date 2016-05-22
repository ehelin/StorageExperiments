using Shared;
using System.Collections.Generic;
using Shared.dto.dynamodb;
using Amazon.DynamoDBv2;
using System;
using Amazon;
using Amazon.DynamoDBv2.Model;

namespace DynamoDb
{
    public class DynamoDbMain : Shared.dto.Main
    {
        public DynamoDbMain() { }

        public DynamoDbMain(string accessKey,
                            string secretKey,
                            int pMaxThreads,
                            long sourceRecordTotal)
        {
            this.Credentials = new DynamoDbStorageCredentials(accessKey, secretKey);
            this.StorageType = Enumeration.StorageTypes.DynamoDb;
            this.MaxThreadsAllowed = pMaxThreads;

            if (sourceRecordTotal > 0)
                this.TotalRecordCount = sourceRecordTotal;
            else
                SetRecordCountTotal();
        }

        public void Run()
        {
            DynamoDbStorageCredentials cred = (DynamoDbStorageCredentials)this.Credentials;
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(cred.accessKey, cred.secretKey, RegionEndpoint.USWest2);

            CreateTable(client);

            Console.WriteLine("Starting to wait for table to be ready...");
            System.Threading.Thread.Sleep(300000); //wait for new table to be ready for input

            Console.WriteLine("Done waiting...starting data load!");
            RunExample();
        }

        private void CreateTable(AmazonDynamoDBClient client)
        {
            bool done = false;
            int ctr = 0;
            while (!done)
            {
                try
                {
                    Console.WriteLine("Creating table...");
                    done = CreateAmazonTable(client);
                    Console.WriteLine("Table created!");
                }
                catch (Exception e)
                {

                    Console.WriteLine("Error: " + e.Message);
                    if (e.Message.IndexOf("Table already exists") != -1)
                    {
                        Console.WriteLine("Deleting table...");
                        DeleteAmazonTable(client);
                        Console.WriteLine("Table deleted!");

                        System.Threading.Thread.Sleep(2000);
                    }
                }

                if (ctr > 5)  //do not try to many times
                    break;
                else
                    ctr++;
            }
        }
        private void DeleteAmazonTable(AmazonDynamoDBClient client)
        {
            DeleteTableRequest dtr = new DeleteTableRequest(Constants.DYNAMO_DB_TABLE_NAME);
            try
            {
                client.DeleteTable(dtr);
            }
            catch (Exception e)
            {
                if (e.Message.IndexOf("Requested resource not found") != -1
                        && e.Message.IndexOf("Attempt to change a resource which is still in use") != -1)
                    throw e;
            }
        }
        private bool CreateAmazonTable(AmazonDynamoDBClient client)
        {
            bool created = false;

            List<KeySchemaElement> kse = new List<KeySchemaElement>();
            kse.Add(new KeySchemaElement("SatelliteId", KeyType.HASH));
            kse.Add(new KeySchemaElement("SatelliteRange", KeyType.RANGE));

            List<AttributeDefinition> ad = new List<AttributeDefinition>();
            ad.Add(new AttributeDefinition("SatelliteId", ScalarAttributeType.N));
            ad.Add(new AttributeDefinition("SatelliteRange", ScalarAttributeType.S));

            ProvisionedThroughput pt = new ProvisionedThroughput(50, 50);

            CreateTableRequest ctr = new CreateTableRequest();
            ctr.TableName = Constants.DYNAMO_DB_TABLE_NAME;
            ctr.KeySchema = kse;
            ctr.ProvisionedThroughput = pt;
            ctr.AttributeDefinitions = ad;

            client.CreateTable(ctr);
            created = true;

            return created;
        }
    }
}
