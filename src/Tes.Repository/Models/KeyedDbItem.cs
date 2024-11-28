// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.ComponentModel.DataAnnotations.Schema;

namespace Tes.Repository.Models
{
    public abstract class KeyedDbItem
    {
        [Column("id")]
        public long Id { get; set; }
    }
}
