using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Tes.Models;

namespace Tes.Repository
{
    internal class TesTaskAzureBlockBlobRepository : IRepository<TesTask>
    {
        private readonly BlockBlobDatabase db = new BlockBlobDatabase();

        public async Task<TesTask> CreateItemAsync(TesTask item)
        {
            await db.CreateItemAsync(item.Id, item, item.State.ToString());
            return item;
        }

        public Task DeleteItemAsync(string id)
        {
            throw new NotImplementedException();
        }

        public async Task<IEnumerable<TesTask>> GetItemsAsync(Expression<Func<TesTask, bool>> predicate)
        {
            var items = new List<TesTask>();

            return await db.GetItemsByStateAsync(GetTesState(predicate));
        }

        public static List<TesState> GetTesStates(Expression<Func<TesTask, bool>> predicate)
        {
            var states = new List<TesState>();

            if (predicate.Body is BinaryExpression binaryExpression)
            {
                if (binaryExpression.Left is MemberExpression memberExpression &&
                    memberExpression.Member.Name == "State" &&
                    binaryExpression.Right is ConstantExpression constantExpression)
                {
                    if (constantExpression.Value is int stateValue)
                    {
                        if (Enum.IsDefined(typeof(TesState), stateValue))
                        {
                            states.Add((TesState)stateValue);
                        }
                    }
                }
                else if (binaryExpression.NodeType == ExpressionType.AndAlso)
                {
                    var leftStates = GetTesStates(binaryExpression.Left);
                    var rightStates = GetTesStates(binaryExpression.Right);
                    states.AddRange(leftStates);
                    states.AddRange(rightStates);
                }
            }

            return states;
        }

        public Task<(string, IEnumerable<TesTask>)> GetItemsAsync(Expression<Func<TesTask, bool>> predicate, int pageSize, string continuationToken)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> TryGetItemAsync(string id, Action<TesTask> onSuccess = null)
        {
            throw new NotImplementedException();
        }

        public Task<TesTask> UpdateItemAsync(TesTask item)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
