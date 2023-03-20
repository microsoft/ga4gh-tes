﻿// <auto-generated />
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;
using Tes.Models;
using Tes.Repository;

#nullable disable

namespace Tes.Migrations
{
    [DbContext(typeof(TesDbContext))]
    [Migration("20230320191719_AddIndexToTesTaskJson")]
    partial class AddIndexToTesTaskJson
    {
        /// <inheritdoc />
        protected override void BuildTargetModel(ModelBuilder modelBuilder)
        {
#pragma warning disable 612, 618
            modelBuilder
                .HasAnnotation("ProductVersion", "7.0.3")
                .HasAnnotation("Relational:MaxIdentifierLength", 63);

            NpgsqlModelBuilderExtensions.UseIdentityByDefaultColumns(modelBuilder);

            modelBuilder.Entity("Tes.Models.TesTaskDatabaseItem", b =>
                {
                    b.Property<long>("Id")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("bigint")
                        .HasColumnName("id");

                    NpgsqlPropertyBuilderExtensions.UseIdentityByDefaultColumn(b.Property<long>("Id"));

                    b.Property<TesTask>("Json")
                        .HasColumnType("jsonb")
                        .HasColumnName("json");

                    b.HasKey("Id")
                        .HasName("pk_testasks");

                    b.HasIndex("Json")
                        .HasDatabaseName("ix_testasks_json");

                    NpgsqlIndexBuilderExtensions.HasMethod(b.HasIndex("Json"), "gin");

                    b.ToTable("testasks", (string)null);
                });
#pragma warning restore 612, 618
        }
    }
}
