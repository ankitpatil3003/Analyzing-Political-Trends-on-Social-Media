[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/QM6TDYML)


# Notes on running :
---
navigate to project 
>conda activate trial (conda environment containing packages)
> python reddit_crawler.py
>python chan_crawler.py
>python _init_.py

# Troubleshoot 
___
Noted failure for booting faktory after ssh reload 
start stop redis for faktory 
> start-stop-daemon --stop --quiet --exec /usr/bin/redis-server <br>
> start-stop-daemon --start --quiet --exec /usr/bin/redis-server

run faktory in development mode :
> faktory -e development 

may require stopping faktory by killing process :
get pid by 
> lsof -i -n
mass murder stuff :
kill -9 $(lsof -t -i tcp:7419)

# Problems :
___
faktory terminates process unexpectedly 
screen functions stop unexpectly 
multiple reboots