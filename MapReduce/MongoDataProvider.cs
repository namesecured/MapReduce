using System;
using MongoDB.Driver;

namespace MapReduce
{
    public class MongoDataProvider : IDataProvider
    {
        private const string ConnectionString = "mongodb://localhost/MapReduce";

        private MongoDatabase database;

        public void Connect()
        {
            throw new NotImplementedException();
        }

        public void Disconnect()
        {
            throw new NotImplementedException();
        }
    }
}