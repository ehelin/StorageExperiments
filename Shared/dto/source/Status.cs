using Newtonsoft.Json;

namespace Shared.dto.source
{
    public class Status : Update
    {
        [JsonProperty]
        public bool PlanetShift { get; set; }

        [JsonProperty]
        public Position SatellitePosition { get; set; }

        [JsonProperty]
        public int SourceX { get; set; }

        [JsonProperty]
        public int SourceY { get; set; }

        [JsonProperty]
        public int DestinationX { get; set; }

        [JsonProperty]
        public int DestinationY { get; set; }

        [JsonProperty]
        public string AscentDirection { get; set; }

        [JsonProperty]
        public bool onStation = false;

        [JsonProperty]
        public bool solarPanelsDeployed = false;

        //for satellite ascent/position maintenance it takes 20 ml of fuel to move one point        
        [JsonProperty]
        public decimal fuel = 0;        //fuel

        //for running communications and non-fuel onboard systems it takes .25 watts to move 
        //power navigation systems to move one point        
        [JsonProperty]
        public decimal power = 0;       //watts

        public Status(int pX,
                      int pY,
                      int pSourceX,
                      int pSourceY,
                      int pDestinationX,
                      int pDestinationY,
                      string pSatelliteName,
                      string pAscentDirection,
                      bool pOnStation,
                      bool pSolarPanelsDeployed,
                      decimal pFuel,
                      decimal pPower)
        {
            SatellitePosition = new Position(pX, pY);

            SourceX = pSourceX;
            SourceY = pSourceY;
            DestinationX = pDestinationX;
            DestinationY = pDestinationY;
            SatelliteName = pSatelliteName;
            AscentDirection = pAscentDirection;
            onStation = pOnStation;
            solarPanelsDeployed = pSolarPanelsDeployed;
            fuel = pFuel;
            power = pPower;
        }

        public override string ToString()
        {
            string val = string.Empty;

            val = "Position (Name/X/Y/Fuel/Power/SourceX/SourceY/DestX/DestY/AscentDir/OnStation/SolarDep/Shifted) ("
                    + this.SatelliteName + "/"
                    + SatellitePosition.X.ToString() + "/"
                    + SatellitePosition.Y.ToString() + "/"
                    + this.fuel.ToString() + "/"
                    + this.power.ToString() + "/"
                    + this.SourceX.ToString() + "/"
                    + this.SourceY.ToString() + "/"
                    + this.DestinationX.ToString() + "/"
                    + this.DestinationY.ToString() + "/"
                    + this.AscentDirection.ToString() + "/"
                    + this.onStation.ToString() + "/"
                    + this.solarPanelsDeployed.ToString() + "/"
                    + this.PlanetShift.ToString()
                    + ")";

            return val;
        }
    }
}
