# kline-fetcher
A tool to fetch all historical kline data into csv file

Cryptocurrency Exchange API's generally put a limit on the amount of data that can be retrieved at once.

Also, it would be cumbersome to repeatedly call the API to access the data every time you do an analysis.

I basically use this application to get the data I need while using the <b>Backtrack</b> library.

When latest data is needed, executing the application again is sufficient to update the current file.
