using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Tes.Migrations
{
    /// <inheritdoc />
    public partial class AddIndicesToJson : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("CREATE INDEX ix_testasks_json_id ON testasks USING HASH (json->'id');");
            migrationBuilder.Sql("CREATE INDEX ix_testasks_json_state ON testasks USING HASH (json->'state');");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("DROP INDEX ix_testasks_json_id;");
            migrationBuilder.Sql("DROP INDEX ix_testasks_json_state;");
        }
    }
}
