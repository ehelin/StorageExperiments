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

            DeleteTable(client);
            CreateTable(client);
            System.Threading.Thread.Sleep(60000);

            RunExample();
        }

        private void DeleteTable(AmazonDynamoDBClient client)
        {
            DeleteTableRequest dtr = new DeleteTableRequest(Constants.DYNAMO_DB_TABLE_NAME);
            try
            {
                client.DeleteTable(dtr);
            }
            catch (Exception e)
            {
                if (e.Message != "Requested resource not found: Table: SatelliteUpdates not found")
                    throw e;
            }
        }
        private void CreateTable(AmazonDynamoDBClient client)
        {
            List<KeySchemaElement> kse = new List<KeySchemaElement>();
            kse.Add(new KeySchemaElement("SatelliteId", KeyType.HASH));
            kse.Add(new KeySchemaElement("SatelliteRange", KeyType.RANGE));

            List<AttributeDefinition> ad = new List<AttributeDefinition>();
            ad.Add(new AttributeDefinition("SatelliteId", ScalarAttributeType.S));
            ad.Add(new AttributeDefinition("SatelliteRange", ScalarAttributeType.S));

            ProvisionedThroughput pt = new ProvisionedThroughput(1, 50);

            CreateTableRequest ctr = new CreateTableRequest();
            ctr.TableName = Constants.DYNAMO_DB_TABLE_NAME;
            ctr.KeySchema = kse;
            ctr.ProvisionedThroughput = pt;
            ctr.AttributeDefinitions = ad;

            client.CreateTable(ctr);
        }
    }
}
