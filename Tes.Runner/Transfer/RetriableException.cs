namespace Tes.Runner.Transfer;

public class RetriableException : Exception
{
    public RetriableException(string message, Exception ex) : base(message, ex)
    {
    }
}
