apt-get update
apt-get install -y python3-pip
apt install -y nodejs
apt install -y xvfb
apt-get install -y wget
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
apt-get -y update
apt-get install -y google-chrome-stable
apt-get install -yqq unzip
wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/
pip3 install numpy
pip3 install pandas
pip3 install adblockparser
pip3 install openpyxl
pip3 install pyvirtualdisplay
pip3 install selenium
pip3 install tldextract
pip3 install webdriver-manager
