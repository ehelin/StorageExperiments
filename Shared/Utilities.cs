using System.Data.SqlClient;
using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Shared.dto.source;
using Shared.dto.SqlServer;
using System.IO;
using Shared.dto;
using Microsoft.Azure.Documents.Client;
using System.Collections.Generic;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace Shared
{
    public class Utilities
    {
        #region IO

        //Based heavily on this post - http://stackoverflow.com/questions/9920804/how-to-list-all-objects-in-amazon-s3-bucket
        public static ListObjectsRequest AnotherMarker(ListObjectsResponse response, ListObjectsRequest request)
        {
            if (response.IsTruncated)
            {
                request.Marker = response.NextMarker;
            }
            else
            {
                request = null;
            }

            return request;
        }
        public static List<string> GetDirectoryFileNames(string localPath)
        {
            List<string> directoryFileNames = new List<string>();

            directoryFileNames.Add(localPath + "\\EastClientUpdate\\EastClientUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\WestClientUpdate\\WestClientUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\NorthClientUpdate\\NorthClientUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\SouthClientUpdate\\SouthClientUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\NorthEastClientUpdate\\NorthEastClientUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\NorthWestClientUpdate\\NorthWestClientUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\SouthWestClientUpdate\\SouthWestClientUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\SouthEastClientUpdate\\SouthEastClientUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\EastStatusUpdate\\EastStatusUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\WestStatusUpdate\\WestStatusUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\NorthStatusUpdate\\NorthStatusUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\SouthStatusUpdate\\SouthStatusUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\NorthEastStatusUpdate\\NorthEastStatusUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\NorthWestStatusUpdate\\NorthWestStatusUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\SouthWestStatusUpdate\\SouthWestStatusUpdateFiles.txt");
            directoryFileNames.Add(localPath + "\\SouthEastStatusUpdate\\SouthEastStatusUpdateFiles.txt");

            return directoryFileNames;
        }

        public static void CloseIoObjects(StreamReader sr, StreamWriter sw)
        {
            if (sw != null)
            {
                sw.Flush();
                sw.Close();
                sw.Dispose();
                sw = null;
            }
            if (sr != null)
            {
                sr.Close();
                sr.Dispose();
                sr = null;
            }
        }

        #endregion

        #region database 

        public static void CloseRdr(SqlDataReader dr)
        {
            if (dr != null)
            {
                dr.Close();
                dr.Dispose();
                dr = null;
            }
        }
        public static SqlCommand GetCommand(DataStorageCredentials credentials)
        {
            SqlServerStorageCredentials bsc = (SqlServerStorageCredentials)credentials;
            SqlConnection conn = new SqlConnection(bsc.dbConnection);
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = conn;

            cmd.Connection.Open();

            return cmd;
        }
        public static DateTime GetSafeDate(object val)
        {
            DateTime result = DateTime.MinValue;

            if (val != DBNull.Value && val != null)
                result = Convert.ToDateTime(val);

            return result;
        }
        public static string GetSafeString(object val)
        {
            string result = string.Empty;

            if (val != DBNull.Value && val != null)
                result = Convert.ToString(val);

            return result;
        }
        public static int GetSafeInt(object val)
        {
            int result = 0;
            int valInt = 0;

            if (val != DBNull.Value && val != null && val.ToString().Length > 0)
            {
                string valStr = val.ToString();
                Int32.TryParse(valStr, out valInt);

                if (valInt != 0)
                    result = valInt;
            }

            return result;
        }
        public static Int64 GetSafeLong(object val)
        {
            Int64 result = 0;
            Int64 valInt = 0;

            if (val != DBNull.Value && val != null && val.ToString().Length > 0)
            {
                string valStr = val.ToString();
                Int64.TryParse(valStr, out valInt);

                if (valInt != 0)
                    result = valInt;
            }

            return result;
        }
        public static void CloseCmd(SqlCommand cmd)
        {
            if (cmd != null)
            {
                if (cmd.Connection != null)
                {
                    if (cmd.Connection.State == System.Data.ConnectionState.Open)
                        cmd.Connection.Close();

                    cmd.Connection.Dispose();
                    cmd.Connection = null;
                }

                cmd.Dispose();
                cmd = null;
            }
        }
        public static void CloseDbObjects(SqlConnection conn,
                                     SqlCommand cmd,
                                     SqlDataReader rdr,
                                     SqlDataAdapter da)
        {
            if (conn != null)
            {
                conn.Close();
                conn.Dispose();
                conn = null;
            }

            if (cmd != null)
            {
                cmd.Dispose();
                cmd = null;
            }

            if (rdr != null)
            {
                rdr.Close();
                rdr.Dispose();
                rdr = null;
            }

            if (da != null)
            {
                da.Dispose();
                da = null;
            }
        }

        #endregion

        #region Serialize/Deserialize

        public static string SerializeSourceRecord(SourceRecord sr)
        {
            string serializedValue = string.Empty;

            serializedValue = JsonConvert.SerializeObject(sr);

            return serializedValue;
        }
        public static SourceRecord DeserializeSourceRecord(string serializedValue)
        {
            SourceRecord sr = null;

            sr = JsonConvert.DeserializeObject<SourceRecord>(serializedValue);

            return sr;
        }
        public static string SerializeStatus(Status s)
        {
            string serializedValue = string.Empty;

            serializedValue = JsonConvert.SerializeObject(s);

            return serializedValue;
        }
        public static Status DeserializeStatus(string serializedValue)
        {
            Status s = null;

            s = JsonConvert.DeserializeObject<Status>(serializedValue);

            return s;
        }
        public static string SerializeSatelliteClient(SatelliteClient sc)
        {
            string serializedValue = string.Empty;

            serializedValue = JsonConvert.SerializeObject(sc);

            return serializedValue;
        }
        public static SatelliteClient DeserializeSatelliteClient(string serializedValue)
        {
            SatelliteClient sc = null;

            sc = JsonConvert.DeserializeObject<SatelliteClient>(serializedValue);

            return sc;
        }

        #endregion

        #region Cloud Clients

        public static CloudTable GetTableStorageContainer(bool create, string azureConnectionString, string azureTableStorageTable)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(azureConnectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            CloudTable table = tableClient.GetTableReference(azureTableStorageTable);
            if (create)
                table.CreateIfNotExists();

            return table;
        }
        public static CloudTableClient GetTableStorageClient(string azureConnectionString)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(azureConnectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            return tableClient;
        }

        public static CloudBlobClient GetBlobStorageClient(string azureConnectionString)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(azureConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            return blobClient;
        }
        public static CloudBlobContainer GetBlobStorageContainer(string azureConnectionString, string azureBlobContainer, bool create)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(azureConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer blob = blobClient.GetContainerReference(azureBlobContainer);
            if (create)
                blob.CreateIfNotExists();

            return blob;
        }
        public static DocumentClient GetDocumentDbClient(string url, string key)
        {
            Uri uri = new Uri(url);
            DocumentClient dc = new DocumentClient(uri, key);

            return dc;
        }

        #endregion
    }
}
