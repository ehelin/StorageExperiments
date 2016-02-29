using Newtonsoft.Json;

namespace Shared.dto.source
{
    public class SatelliteClient : Update
    {
        [JsonProperty]
        public bool Onstation { get; set; }

        [JsonProperty]
        public bool SolarPanelsDeployed { get; set; }

        [JsonProperty]
        public bool PlanetShift { get; set; }

        [JsonProperty]
        public int DestinationX { get; set; }

        [JsonProperty]
        public int DestinationY { get; set; }

        public SatelliteClient(string pSatelliteName, bool pOnstation, bool pSolarPanelsDeployed)
        {
            SatelliteName = pSatelliteName;
            Onstation = pOnstation;
            SolarPanelsDeployed = pSolarPanelsDeployed;
        }
    }
}
