// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Tes.Utilities
{
    internal class ExpressionParameterSubstitute : ExpressionVisitor
    {
        private readonly ParameterExpression _from;
        private readonly Expression _to;

        public ExpressionParameterSubstitute(ParameterExpression from, Expression to)
        {
            _from = from;
            _to = to;
        }

        protected override Expression VisitLambda<T>(Expression<T> node)
        {
            if (node.Parameters.All(p => p != _from))
                return node;

            // We need to replace the `from` parameter, but in its place we need the `to` parameter(s)
            // e.g. F<DateTime,Bool> subst F<Source,DateTime> => F<Source,bool>
            // e.g. F<DateTime,Bool> subst F<Source1,Source2,DateTime> => F<Source1,Source2,bool>

            var toLambda = _to as LambdaExpression;
            var substituteParameters = toLambda?.Parameters ?? Enumerable.Empty<ParameterExpression>();

            System.Collections.ObjectModel.ReadOnlyCollection<ParameterExpression> substitutedParameters
                = new System.Collections.ObjectModel.ReadOnlyCollection<ParameterExpression>(node.Parameters
                    .SelectMany(p => p == _from ? substituteParameters : Enumerable.Repeat(p, 1))
                    .ToList());

            var updatedBody = Visit(node.Body);        // which will convert parameters to 'to'
            return Expression.Lambda(updatedBody, substitutedParameters);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            var toLambda = _to as LambdaExpression;
            if (node == _from) return toLambda?.Body ?? _to;
            return base.VisitParameter(node);
        }
    }
}
