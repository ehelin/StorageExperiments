using System.Runtime.Serialization;

namespace Shared.dto.EventHub
{
    [DataContract]
    public class SourceRecordEventHub
    {
        [DataMember]
        public long Id { get; set; }

        [DataMember]
        public string Type { get; set; }

        [DataMember]
        public string Data { get; set; }

        [DataMember]
        public System.DateTime Created { get; set; }
    }
}