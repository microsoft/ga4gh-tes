using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace Tes.Models
{
    /// <summary>
    /// Database schema for encapsulating a TesTask as Json for Postgresql.
    /// </summary>
    [Table("tesktaskdatabaseitems")]
    public class TeskTaskDatabaseItem
    {
        [Column("id")]
        public long Id { get; set; }
        [Column("json", TypeName = "jsonb")]
        public TesTask Json { get; set; }
    }
}
