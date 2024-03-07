# Description
1) Conducted in-depth analysis of social media trends using data analytics and sentiment analysis, revealing prevalent political currents and public sentiments in the digital age.
2) Applied data-driven techniques to identify patterns, trends, and sentiments on platforms such as Reddit and 4Chan, offering insights into diverse perspectives, political affiliations, and emotional dynamics in contemporary political engagement.
3) Investigated and addressed the prevalence, origins, and impact of hate speech and online abuse in the online political landscape, developing strategies to mitigate harm and contributing to a nuanced understanding of digitally connected political discourse.

# Team Members
1) Ankit Patil
2) Paritosh Marathe
3) Piyush Rathod
4) Parinita Chinchalikar
5) Danashree Borkar

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
