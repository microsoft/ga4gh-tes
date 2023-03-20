using System;

namespace Tes.Repository
{
    public class DatabaseOverloadedException : Exception
    {
        public override string Message => "The database is currently overloaded; consider scaling the database up or reduce the number of requests";
    }
}
