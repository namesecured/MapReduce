using MongoDB.Driver;

namespace MapReduce
{
    public class MongoDataProvider : IDataProvider
    {
        private const string ConnectionString = "mongodb://localhost/MapReduce";

        private MongoClient client;
        private MongoServer server;

        public void Connect()
        {
            this.client = new MongoClient(ConnectionString);
            this.server = this.client.GetServer();
        }

        public void Disconnect()
        {
            if (this.server.State != MongoServerState.Disconnected)
            {
                this.server.Disconnect();
            }
        }
    }
}