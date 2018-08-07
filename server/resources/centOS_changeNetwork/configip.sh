#|/bin/bash
#declare variable
FILE="ifcfg-eth0"
#instructions
sudo rm -f /etc/sysconfig/network-scripts/$FILE
sudo cp /home/cloudera/code/$FILE /etc/sysconfig/network-scripts
