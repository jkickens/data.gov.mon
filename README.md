# data.gov.mon
Python program to monitor links on data.gov

The program takes as input a search term (defaults to "climate"), searches data.gov for all resources matching the search term, then 
attempts to access each resource, counting bad links.  

The program uses Python multithreading to speed up processing, most of which is taken up waiting for data resources to respond.

Output is a summary of how many links were followed and whether the links were good or bad, and whether HTTP or FTP (as well as how long processing took).
A CSV file is also created containing the URLs followed with the description provided by data.gov and for HTTP the actual 
response code returned.

Example output and CSV file for default search term "climate" is provided.

Attempts are not made to actually download all the resources.  That would take forever!


