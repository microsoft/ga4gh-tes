// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using System.CommandLine.Binding;
using System.CommandLine.Invocation;
using System.CommandLine.Parsing;

namespace Tes.RunnerCLI.Commands
{
    // TODO: This will need to be updated or removed when System.CommandLine is updated.

    /// <summary>
    /// Extends <see cref="Handler"/>
    /// </summary>
    /// <remarks>
    /// This will need to be updated or removed when System.CommandLine is updated.
    /// </remarks>
    internal static class HandlerEx
    {
        public static void SetHandler<T1, T2, T3, T4, T5, T6, T7, T8, T9>(
            this Command command,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, Task> handle,
            IValueDescriptor<T1> symbol1,
            IValueDescriptor<T2> symbol2,
            IValueDescriptor<T3> symbol3,
            IValueDescriptor<T4> symbol4,
            IValueDescriptor<T5> symbol5,
            IValueDescriptor<T6> symbol6,
            IValueDescriptor<T7> symbol7,
            IValueDescriptor<T8> symbol8,
            IValueDescriptor<T9> symbol9)
        {
            command.SetHandler(context =>
            {
                var value1 = GetValueForHandlerParameter(symbol1, context);
                var value2 = GetValueForHandlerParameter(symbol2, context);
                var value3 = GetValueForHandlerParameter(symbol3, context);
                var value4 = GetValueForHandlerParameter(symbol4, context);
                var value5 = GetValueForHandlerParameter(symbol5, context);
                var value6 = GetValueForHandlerParameter(symbol6, context);
                var value7 = GetValueForHandlerParameter(symbol7, context);
                var value8 = GetValueForHandlerParameter(symbol8, context);
                var value9 = GetValueForHandlerParameter(symbol9, context);

                return handle(value1!, value2!, value3!, value4!, value5!, value6!, value7!, value8!, value9!);
            });
        }

        public static void SetHandler<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(
            this Command command,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, Task> handle,
            IValueDescriptor<T1> symbol1,
            IValueDescriptor<T2> symbol2,
            IValueDescriptor<T3> symbol3,
            IValueDescriptor<T4> symbol4,
            IValueDescriptor<T5> symbol5,
            IValueDescriptor<T6> symbol6,
            IValueDescriptor<T7> symbol7,
            IValueDescriptor<T8> symbol8,
            IValueDescriptor<T9> symbol9,
            IValueDescriptor<T10> symbol10)
        {
            command.SetHandler(context =>
            {
                var value1 = GetValueForHandlerParameter(symbol1, context);
                var value2 = GetValueForHandlerParameter(symbol2, context);
                var value3 = GetValueForHandlerParameter(symbol3, context);
                var value4 = GetValueForHandlerParameter(symbol4, context);
                var value5 = GetValueForHandlerParameter(symbol5, context);
                var value6 = GetValueForHandlerParameter(symbol6, context);
                var value7 = GetValueForHandlerParameter(symbol7, context);
                var value8 = GetValueForHandlerParameter(symbol8, context);
                var value9 = GetValueForHandlerParameter(symbol9, context);
                var value10 = GetValueForHandlerParameter(symbol10, context);

                return handle(value1!, value2!, value3!, value4!, value5!, value6!, value7!, value8!, value9!, value10!);
            });
        }

        public static void SetHandler<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(
            this Command command,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, Task> handle,
            IValueDescriptor<T1> symbol1,
            IValueDescriptor<T2> symbol2,
            IValueDescriptor<T3> symbol3,
            IValueDescriptor<T4> symbol4,
            IValueDescriptor<T5> symbol5,
            IValueDescriptor<T6> symbol6,
            IValueDescriptor<T7> symbol7,
            IValueDescriptor<T8> symbol8,
            IValueDescriptor<T9> symbol9,
            IValueDescriptor<T10> symbol10,
            IValueDescriptor<T11> symbol11)
        {
            command.SetHandler(context =>
            {
                var value1 = GetValueForHandlerParameter(symbol1, context);
                var value2 = GetValueForHandlerParameter(symbol2, context);
                var value3 = GetValueForHandlerParameter(symbol3, context);
                var value4 = GetValueForHandlerParameter(symbol4, context);
                var value5 = GetValueForHandlerParameter(symbol5, context);
                var value6 = GetValueForHandlerParameter(symbol6, context);
                var value7 = GetValueForHandlerParameter(symbol7, context);
                var value8 = GetValueForHandlerParameter(symbol8, context);
                var value9 = GetValueForHandlerParameter(symbol9, context);
                var value10 = GetValueForHandlerParameter(symbol10, context);
                var value11 = GetValueForHandlerParameter(symbol11, context);

                return handle(value1!, value2!, value3!, value4!, value5!, value6!, value7!, value8!, value9!, value10!, value11!);
            });
        }

        private static T? GetValueForHandlerParameter<T>(
            IValueDescriptor<T> symbol,
            InvocationContext context)
        {
            if (symbol is IValueSource valueSource &&
                valueSource.TryGetValue(symbol, context.BindingContext, out var boundValue) &&
                boundValue is T value)
            {
                return value;
            }
            else
            {
                return context.ParseResult.GetValueFor(symbol);
            }
        }

        private static T? GetValueFor<T>(this ParseResult parseResult, IValueDescriptor<T> symbol)
        {
            return symbol switch
            {
                Argument<T> argument => parseResult.GetValueForArgument(argument),
                Option<T> option => parseResult.GetValueForOption(option),
                _ => throw new ArgumentOutOfRangeException(nameof(symbol)),
            };
        }
    }
}
