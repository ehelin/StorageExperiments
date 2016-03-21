using Newtonsoft.Json;

namespace Shared.dto.source
{
    public class SourceRecord
    {
        [JsonProperty]
        public long Id { get; set; }

        [JsonProperty]
        public string Type { get; set; }

        [JsonProperty]
        public string Data { get; set; }

        [JsonProperty]
        public System.DateTime Created { get; set; }
    }
}
