using System;
using MongoDB.Bson;

namespace MapReduce
{
    public class StatisticItem
    {
        public DateTime DateTime { get; set; }

        public DateTime StartDate { get; set; }

        public DateTime EndDate { get; set; }

        public string SessionId { get; set; }

        public int Count { get; set; }
    }
}