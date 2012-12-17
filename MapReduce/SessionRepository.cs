using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
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

        public IEnumerable<Session> Get()
        {
            return this.Storage.All().ToList();
        }

        public IEnumerable<Session> Get(Expression<Func<Session, bool>> func)
        {
            return this.Storage.All(func).ToList();
        }
    }
}