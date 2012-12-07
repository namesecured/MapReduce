using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MapReduce
{    
    public interface IDataProvider
    {
        void Connect();
        void Disconnect();
    }
}
