﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Transfer;

public class RetriableException(string message, Exception ex) : Exception(message, ex)
{ }
