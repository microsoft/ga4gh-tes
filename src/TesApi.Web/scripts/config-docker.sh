#!/usr/bin/bash
trap "echo Error trapped; exit 0" ERR
# set -e will cause any error to exit the script
set -e
sudo touch tmp2.json
sudo cp /etc/docker/daemon.json tmp1.json || sudo echo {} > tmp1.json
sudo chmod a+w tmp?.json;
if fgrep "$(dirname "$(dirname "$AZ_BATCH_NODE_ROOT_DIR")")/docker" tmp1.json; then
    echo grep "found docker path"
elif [ $? -eq 1 ]; then
    {PackageInstalls}
    python3 <<INNEREOF
import json,os
data=json.load(open("tmp1.json"))
data["data-root"]=os.path.join(os.path.dirname(os.path.dirname(os.getenv("AZ_BATCH_NODE_ROOT_DIR"))), "docker")
json.dump(data, open("tmp2.json", "w"), indent=2)
INNEREOF
    sudo cp tmp2.json /etc/docker/daemon.json
    sudo chmod 644 /etc/docker/daemon.json
    sudo systemctl restart docker
    echo "updated docker data-root"
else
    echo "grep failed" || exit 1
fi
