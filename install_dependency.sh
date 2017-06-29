#!/bin/bash

sudo apt-get update
sudo apt-get upgrade

sudo apt-get update
sudo apt-get upgrade

# Install Python3.5
echo "Install python 3.5"
sudo add-apt-repository ppa:fkrull/deadsnakes
sudo apt-get update
sudo apt-get install python3.5

# Install setup tool
echo "Install setup tool"
wget https://bootstrap.pypa.io/ez_setup.py -O - | sudo python3.5

# Install Colors
echo "Install Color"
git clone https://github.com/verigak/colors.git
cd color
sudo python3.5 setup.py install

# Install REST framework (Falcon)
echo "Install REST framework (Falcon)"
git clone https://github.com/falconry/falcon.git
# install on python 3.5 (The one that you just has installed)
cd falcon
sudo python3.5 setup.py install

echo "Install docker-py library"
echo "Install docker-py"
git clone https://github.com/docker/docker-py.git
cd docker-py
sudo python3.5 setup.py install