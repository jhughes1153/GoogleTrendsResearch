# GoogleTrends

Google Trends Keywords:
The whole premise of this project is to see if one can predict stock prices using google trends.

This project has two classes one that gets stock prices and dividend info from yahoo and one that gets google keywords historical data from yahoo.
These are then sent to a local SQL Server database on my local machine so that I do not have to rely as much on webscraping
From there I have a server and client setup that will handle requests to and from the server.
The machine learning algorithms will be the next step and will be added later unless there is a way to predict stock prices using this method.
In some cases this program could be used instead of the pandas datareader which was I used in an older project, but this one does not get the data from a certain date but for the last 5 years, but it could be changed easily to get any date like pandas datareader did.
