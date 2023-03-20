using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Tes.Migrations
{
    /// <inheritdoc />
    public partial class AddIndexToTesTaskJson : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateIndex(
                name: "ix_testasks_json",
                table: "testasks",
                column: "json")
                .Annotation("Npgsql:IndexMethod", "gin");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "ix_testasks_json",
                table: "testasks");
        }
    }
}
