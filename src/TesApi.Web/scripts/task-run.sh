sudo docker rm $(docker ps -aq) || :
sudo docker image prune -a || :
sudo find $AZ_BATCH_NODE_ROOT_DIR/workitems -maxdepth 5 -path $(dirname $(dirname $(dirname $PWD))) -prune -o -type d -name wd -execdir rm -fdr '{}' \; || :
sudo docker system prune --volumes -f || :
/bin/sh {BatchScriptPath}
