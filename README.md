# My-G-Photo
This is a console application that gets media files and media metadata from your Google Photo storage to your local storage.

## Requirements
Unix-like OS with Python3.6 or beyond.
Python external modules:
- Requests: ```pip install requests```


Not tested on Windows OS.

## Installation
### Debian/Ubuntu
- Install requirements:
```
sudo apt update && sudo apt install python3.8 unzip git
python3.8 -m pip install --upgrade pip requests
```
- Download the application:
https://github.com/Dionis-90/my-g-photo/archive/refs/heads/main.zip
or ```git clone git@github.com:Dionis-90/my-g-photo.git```
- Unpack it (if you used web url):
```
unzip main.zip
```
and enter the folder
```
cd my-g-photo
```
- Create database and config file:
```
cp db.sqlite.structure db.sqlite
cp config.py.example config.py
```
- Open the config file in your favorite editor and change settings if needed.
- Run the app:
```
python3.8 app.py &
```

Also you can to set up crontab or anacrontab.
Example for daily anacron:
Create file:
```
nano /etc/cron.daily/my-g-photo
```
and put there:
```
#!/bin/bash
HOME=/home/{Your user name}
LOGUSER={Your user name}
cd "{Path to application dir}"
su {Your user name} -c "{Path to application dir}/app.py"
```
