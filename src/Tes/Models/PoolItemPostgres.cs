using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace Tes.Models
{
    /// <summary>
    /// Placeholder database schema for encapsulating a PoolItem as Json for Postgresql.
    /// TODO(bmurri): adding pool events to postgres
    /// </summary>
    [Table("pooldatabaseitem")]
    public class PoolDatabaseItem
    {
        [Column("id")]
        public long Id { get; set; }
        [Column("json", TypeName = "jsonb")]
        public string Json { get; set; }
    }
}
