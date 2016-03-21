using System;
using Shared.dto.source;
using Shared.dto.threading;
using Microsoft.ServiceBus.Messaging;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Shared.dto.EventHub
{
    public class EventHubLoadThreadJob : ThreadJob
    {
        public EventHubLoadThreadJob() : base() { }

        public EventHubLoadThreadJob(DataStorageCredentials pCredentials,
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
                        SourceRecordEventHub srch = ConvertSourceRecord(sr);
                        //InsertRecord(srch);
                        InsertRecordAsycn(srch);
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
        private SourceRecordEventHub ConvertSourceRecord(SourceRecord sr)
        {
            SourceRecordEventHub srch = new SourceRecordEventHub();

            srch.Created = sr.Created;
            srch.Data = sr.Data;
            srch.Id = sr.Id;
            srch.Type = sr.Type;

            return srch;
        }
        private void InsertRecordAsycn(Shared.dto.EventHub.SourceRecordEventHub u)
        {
            EventHubStorageCredentials ehsc = (EventHubStorageCredentials)Credentials;
            EventHubClient client = EventHubClient.CreateFromConnectionString(ehsc.eventHub, ehsc.eventHubName);
            var serializedString = JsonConvert.SerializeObject(u);

            EventData data = new EventData(System.Text.Encoding.UTF8.GetBytes(serializedString))
            {
                PartitionKey = "$Default"
            };

            data.Properties.Add("Type", "SatelliteUpdate");

            int errorTryCtr = 0;
            bool isSent = false;
            while (!isSent)
            {
                try
                {
                    client.SendAsync(data);
                    isSent = true;
                }
                catch (Exception e)
                {
                    if (errorTryCtr > Constants.MAX_RETRY)
                    {
                        Console.WriteLine("EventHub - Max error limit reached...moving on");
                        break;
                    }
                    else
                    {
                        Console.WriteLine("EventHub ERROR: - " + e.Message);
                        errorTryCtr++;
                    }
                }
                finally
                {
                    if (data != null)
                    {
                        data.Dispose();
                        data = null;
                    }

                    if (client != null)
                    {
                        client.Close();
                        client = null;
                    }
                }
            }
        }

        private void InsertRecord(Shared.dto.EventHub.SourceRecordEventHub u)
        {
            EventHubStorageCredentials ehsc = (EventHubStorageCredentials)Credentials;
            EventHubClient client = EventHubClient.CreateFromConnectionString(ehsc.eventHub, ehsc.eventHubName);
            var serializedString = JsonConvert.SerializeObject(u);

            EventData data = new EventData(System.Text.Encoding.UTF8.GetBytes(serializedString))
            {
                PartitionKey = "$Default"
            };

            data.Properties.Add("Type", "SatelliteUpdate");

            int errorTryCtr = 0;
            bool isSent = false;
            while (!isSent)
            {
                try
                {

                    client.Send(data);
                    isSent = true;
                }
                catch (Exception e)
                {
                    if (errorTryCtr > Constants.MAX_RETRY)
                    {
                        Console.WriteLine("EventHub - Max error limit reached...moving on");
                        break;
                    }
                    else
                    {
                        Console.WriteLine("EventHub ERROR: - " + e.Message);
                        errorTryCtr++;
                    }
                }
                finally
                {
                    if (data != null)
                    {
                        data.Dispose();
                        data = null;
                    }

                    if (client != null)
                    {
                        client.Close();
                        client = null;
                    }
                }
            }
        }
        protected override void GetRecordCount(DataStorageCredentials pCredentials)
        {
            Console.WriteLine("Event Hub Total is not calculated.  Get from stream analytics job query - " + DateTime.Now.ToString());
        }
    }
}

