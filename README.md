# My-G-Photo
This is a console application that gets media files and media metadata from your Google Photo storage to your local storage.

## Requirements
Unix-like OS with Python3.6 or beyond.
Python external modules:
See: `requirements.txt`


Not tested on Windows OS.

## Installation
### Debian/Ubuntu
- Download the application:
https://github.com/Dionis-90/my-g-photo/archive/refs/heads/main.zip
or ```git clone git@github.com:Dionis-90/my-g-photo.git```
- Unpack it (if you used web url):
```
unzip main.zip
```
- Enter the folder
```
cd my-g-photo || cd my-g-photo-main
```
- Install requirements:
```
python3 -m pip install -r requirements.txt
```
- Create config file:
```
cp app/config/config.py.example app/config/config.py
```
- Open the config file in your favorite editor and change settings if needed.
- Run the app:
```
python3 main.py &
```

Also, you can run app by systemd, see example: `app/config/my-g-photo.service`
