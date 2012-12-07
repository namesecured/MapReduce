using MongoRepository;

namespace MapReduce
{    
    public class SessionRepository : ISessionRepository
    {
        private const string ConnectionString = "mongodb://localhost/MapReduce";
        private MongoRepository<Session> storage;

        private MongoRepository<Session> Storage { get { return this.storage ?? (this.storage = new MongoRepository<Session>(ConnectionString)); } }

        public void Add(Session session)
        {
            this.Storage.Add(session);
        }
    }
}