// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;

namespace Tes.Repository.Utilities
{
    internal class PrependableFormattableString : FormattableString
    {
        private readonly FormattableString source;
        private readonly string prefix;

        public PrependableFormattableString(string prefix, FormattableString formattableString)
        {
            ArgumentNullException.ThrowIfNull(formattableString);
            ArgumentException.ThrowIfNullOrEmpty(prefix);

            source = formattableString;
            this.prefix = prefix;
        }

        public override int ArgumentCount => source.ArgumentCount;

        public override string Format => prefix + source.Format;

        public override object GetArgument(int index)
        {
            return source.GetArgument(index);
        }

        public override object[] GetArguments()
        {
            return source.GetArguments();
        }

        public override string ToString(IFormatProvider formatProvider)
        {
            return prefix + source.ToString(formatProvider);
        }
    }
}
