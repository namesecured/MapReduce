using System;
using System.Globalization;
using MongoRepository;

namespace MapReduce
{
    [CollectionName("Sessions")]
    public class Session : Entity
    {
        public Session(DateTime startDate, DateTime endDate)
        {
            this.SessionId = Guid.NewGuid();
            this.StartDate = startDate;
            this.EndDate = endDate;
        }

        public Guid SessionId { get; private set; }

        public DateTime StartDate { get; private set; }

        public DateTime EndDate { get; private set; }

        public TimeSpan Duration { get { return this.EndDate.Subtract(this.StartDate); } }

        public override string ToString()
        {
            return string.Format("{0} {1} {2}", this.StartDate.ToString("HH:mm", CultureInfo.CreateSpecificCulture("en-US")), this.EndDate.ToString("HH:mm", CultureInfo.CreateSpecificCulture("en-US")), this.Duration.ToString());
        }
    }
}