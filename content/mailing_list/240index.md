+++
title = "240index"
date = 2019-03-23T15:45:41+05:30
weight = 5
chapter = true
hidden = true
+++

*<sub><sub>Amanda.org is an open source site. Please refer the [TO DO LIST](/to_do) to contribute to this site.</sub></sub>*


After 2.4.0, the structure of the directory holding the index files was changed to have three levels instead of being flat. This greatly reduces the number of files in a given directory, which was a problem for some systems.
The new layout is:

[indexdir]/hostname/filesystem/YYYYMMDD_L.gz
where hostname and filesystem are "sanitized" versions of the names from disklist, i.e. '/' characters are converted to '_' and so on. This new naming convention matches the one used for the text formatted database.
A script is available to convert the flat directory structure to the new layout: [msg00428.html](../msg00428)

$ Last updated: $Date: 2002/06/10 15:03:28 $