using Microsoft.WindowsAzure.Storage.Table;
using System.Runtime.Serialization;

namespace Shared.dto.tablestorage
{
    [DataContract]
    public class SourceRecordTableStorage : TableEntity
    {
        [DataMember]
        public long Id { get; set; }

        [DataMember]
        public string Type { get; set; }

        [DataMember]
        public string Data { get; set; }

        [DataMember]
        public System.DateTime Created { get; set; }

        public void SetParitionKeyRowKey()
        {
            if (!string.IsNullOrEmpty(Type))
            {
                string satelliteName = Type.Substring(0, Type.IndexOf("_"));
                
                string rowKey = satelliteName + "_" + Type + "_" + Id.ToString();

                this.PartitionKey = satelliteName;
                this.RowKey = rowKey;

            }

            if (string.IsNullOrEmpty(this.PartitionKey) || string.IsNullOrEmpty(this.RowKey))
            {
                throw new System.Exception("Partition or row key is null or empty. Type:" + Type);
            }
        }
    }
}
