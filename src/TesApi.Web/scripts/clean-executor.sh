cd "$(dirname "$0")"
if [ "$1" != "{TaskExecutor}" ]; then docker rmi {TaskExecutor} -f; fi || :
rm -fdr wd/{ExecutionPathPrefix} || :
sudo docker container prune -f || :
sudo docker system prune -a --volumes -f || :