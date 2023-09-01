// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Tes.Migrations
{
    /// <inheritdoc />
    public partial class AddGinIndex : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("CREATE INDEX ix_testasks_json_tags ON testasks USING GIN ((json->'Tags'));");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("DROP INDEX ix_testasks_json_tags;");
        }
    }
}
