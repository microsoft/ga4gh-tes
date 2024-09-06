// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Immutable;

namespace Tes.ApiClients.Models.Pricing;

/// <summary>
/// Storage disk primary cost information.
/// </summary>
/// <param name="name">Meter name.</param>
/// <param name="capacity">Disk capacity in GiB.</param>
/// <param name="price">Hourly cost.</param>
public readonly record struct StorageDiskPriceInformation()
{
    // Virtual Machines are priced in hours. Storage is priced in months. The conversion factor, based on 24 hours per "ephemeris/conventional mean solar" day (86,400 seconds) is:
    //     24 hours/day x (365.2425 [Gregorian calendar] or 365.242190402 [measured]) days/tropical year / 12 months/year = hours/month
    //     https://www.math.net/days-in-a-year
    //     https://hpiers.obspm.fr/eop-pc/models/constants.html
    private static readonly double HoursPerMonth = (24 / 12) * 365.2425;

    // https://learn.microsoft.com/azure/virtual-machines/disks-types#standard-ssd-size
    // https://azure.microsoft.com/pricing/details/managed-disks/
    private static readonly IEnumerable<KeyValuePair<string, int>> standardLrsSsdCapacities =
        [
            new ("E1 LRS Disk", 4),
            new ("E2 LRS Disk", 8),
            new ("E3 LRS Disk", 16),
            new ("E4 LRS Disk", 32),
            new ("E6 LRS Disk", 64),
            new ("E10 LRS Disk", 128),
            new ("E15 LRS Disk", 256),
            new ("E20 LRS Disk", 512),
            new ("E30 LRS Disk", 1024),
            new ("E40 LRS Disk", 2048),
            new ("E50 LRS Disk", 4096),
            new ("E60 LRS Disk", 8192),
            new ("E70 LRS Disk", 16384),
            new ("E80 LRS Disk", 32767),
        ];

    /// <summary>
    /// Initializes a new instance of the <see cref="StorageDiskPriceInformation"/> struct.
    /// </summary>
    /// <param name="name">Meter name.</param>
    /// <param name="capacity">Disk capacity in GiB.</param>
    /// <param name="price">Hourly cost.</param>
    public StorageDiskPriceInformation(string name, int capacity, decimal price)
        : this()
    {
        MeterName = name;
        CapacityInGiB = capacity;
        PricePerHour = price;
    }

    /// <summary>
    /// Meter name.
    /// </summary>
    public string MeterName { get; }

    /// <summary>
    /// Capacity in GiB.
    /// </summary>
    public int CapacityInGiB { get; }

    /// <summary>
    /// Price per hour.
    /// </summary>
    public decimal PricePerHour { get; }

    /// <summary>
    /// The capacities of each disk by meter name.
    /// </summary>
    public static ImmutableDictionary<string, int> StandardLrsSsdCapacityInGiB { get; } = standardLrsSsdCapacities.ToImmutableDictionary(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// The meter names of each disk by capacity.
    /// </summary>
    public static ImmutableDictionary<int, string> StandardLrsSsdMeterName { get; } = standardLrsSsdCapacities.Select(item => new KeyValuePair<int, string>(item.Value, item.Key)).ToImmutableDictionary();

    /// <summary>
    /// Converts a price per month to price per hour.
    /// </summary>
    /// <param name="pricePerMonth">The price per month.</param>
    /// <returns>The price per hour.</returns>
    public static float ConvertPricePerMonthToPricePerHour(float pricePerMonth)
        => pricePerMonth / (float)HoursPerMonth;
}
