sudo echo -n 'if [ "$1" != "{TaskExecutor}" ]; then docker rmi {TaskExecutor} -f; fi;' > ../clean-executor.sh
sudo echo -n ' rm -fdr wd/{ExecutionPathPrefix} || :' >> ../clean-executor.sh
sudo chmod a+x ../clean-executor.sh
sudo find $AZ_BATCH_NODE_ROOT_DIR/workitems -maxdepth 5 -path $(dirname $(dirname $(dirname $PWD))) -prune -o -type f -name clean-executor.sh -execdir '{}' {TaskExecutor} \; || :
sudo docker system prune --volumes -f || :
/bin/sh {BatchScriptPath}