﻿using Newtonsoft.Json;

namespace Shared.dto.source
{
    public class Position
    {
        [JsonProperty]
        public int X { get; set; }

        [JsonProperty]
        public int Y { get; set; }

        public Position(int pX, int pY)
        {
            X = pX;
            Y = pY;
        }
    }
}
