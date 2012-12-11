using System.Collections.Generic;

namespace MapReduce
{
    public interface ISessionRepository
    {
        void Add(Session session);

        IEnumerable<Session> Get();
    }
}