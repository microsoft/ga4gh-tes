// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Storage;

public static class TerraConfigConstants
{
    public const int TokenExpirationInSeconds = 3600 * 24; //1 day, max time allowed by Terra. 
    public const int CacheExpirationInSeconds = TokenExpirationInSeconds - 1800; // 30 minutes less than token expiration
}
