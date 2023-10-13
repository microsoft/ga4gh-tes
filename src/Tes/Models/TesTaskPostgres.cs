// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace Tes.Models
{
    /// <summary>
    /// Database schema for encapsulating a TesTask as Json for Postgresql.
    /// </summary>
    [Table("testasks")]
    public class TesTaskDatabaseItem
    {
        [Column("id")]
        public long Id { get; set; }

        [Column("json", TypeName = "jsonb")]
        public TesTask Json { get; set; }
    }
}
