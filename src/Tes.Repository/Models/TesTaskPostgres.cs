// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace Tes.Repository.Models
{
    /// <summary>
    /// Database schema for encapsulating a TesTask as Json for Postgresql.
    /// </summary>
    [Table(TesDbContext.TesTasksPostgresTableName)]
    public class TesTaskDatabaseItem : KeyedDbItem
    {
        [Column("json", TypeName = "jsonb")]
        public Tes.Models.TesTask Json { get; set; }

        public TesTaskDatabaseItem Clone()
        {
            var result = (TesTaskDatabaseItem)MemberwiseClone();
            result.Json = Json.Clone();
            return result;
        }
    }
}
