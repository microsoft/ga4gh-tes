// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;

namespace TesApi.Web.Extensions
{
    /// <summary>
    /// <see cref="IEnumerable{T}"/> extensions
    /// </summary>
    public static class EnumerableExtensions
    {
        /// <summary>
        /// Filters a sequence of values based on a false predicate.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of <paramref name="source"/>.</typeparam>
        /// <param name="source">An <see cref="IEnumerable{T}"/> to filter.</param>
        /// <param name="predicate">A function to test each element for a false condition.</param>
        /// <returns>An <see cref="IEnumerable{T}"/> that contains elements from <paramref name="source"/> that do not satisfy the condition.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is null.</exception>
        public static IEnumerable<TSource> WhereNot<TSource>(this IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            ArgumentNullException.ThrowIfNull(predicate);

            return source.Where(element => !predicate(element));
        }

        /// <summary>
        /// Projects each element of a sequence into a new form for each element where a specified condition is true.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of <paramref name="source"/>.</typeparam>
        /// <typeparam name="TResult">The type of the value returned by <paramref name="predicateSelector"/>.</typeparam>
        /// <param name="source">A sequence of values to test and transform.</param>
        /// <param name="predicateSelector">A function to apply to each element which will test for a condition and provide a transformed value.</param>
        /// <returns>An <see cref="IEnumerable{T}"/> whose elements are the result of invoking the transform function on each element that satisfies the condition.</returns>
        public static IEnumerable<TResult> SelectWhere<TSource, TResult>(this IEnumerable<TSource> source, PredicateSelector<TSource, TResult> predicateSelector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(predicateSelector);

            foreach (var element in source)
            {
                if (predicateSelector(element, out var result))
                {
                    yield return result;
                }
            }
        }

        /// <summary>
        /// A combined predicate and selector for <see cref="SelectWhere{TSource, TResult}(IEnumerable{TSource}, PredicateSelector{TSource, TResult})"/>.
        /// </summary>
        /// <typeparam name="TSource">The type of <paramref name="source"/>.</typeparam>
        /// <typeparam name="TResult">The type of <paramref name="result"/>.</typeparam>
        /// <param name="source">Source value.</param>
        /// <param name="result">Transformed value.</param>
        /// <returns>True if the source is transformed.</returns>
        public delegate bool PredicateSelector<TSource, TResult>(TSource source, out TResult result);
    }
}
